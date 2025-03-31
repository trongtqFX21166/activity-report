from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pymongo import MongoClient, UpdateOne, InsertOne
from datetime import datetime
import pytz
import sys


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("recalculate-yearly-membership") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.connection.uri", "mongodb://192.168.10.97:27017") \
        .enableHiveSupport() \
        .getOrCreate()


def get_mongodb_connection():
    """Create MongoDB client connection"""
    try:
        client = MongoClient('mongodb://192.168.10.97:27017/')
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        raise


def get_target_year():
    """Get target year from command line args or use current year"""
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    else:
        return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).year


def recalculate_yearly_data(target_year):
    """Recalculate yearly data by summing all months' points and using latest membership info"""
    spark = None
    mongo_client = None

    try:
        print(f"Starting recalculation of yearly data for {target_year}")

        # Create connections
        spark = create_spark_session()
        mongo_client = get_mongodb_connection()

        # Source and target DB names
        monthly_db = 'activity_membershiptransactionmonthly_dev'
        yearly_db = 'activity_membershiptransactionyearly_dev'

        # Yearly collection name
        yearly_collection = f"{target_year}"

        # 1. Read all monthly collections for the target year
        all_monthly_dfs = []

        for month in range(1, 13):
            monthly_collection = f"{target_year}_{month}"
            try:
                monthly_df = spark.read \
                    .format("mongodb") \
                    .option("connection.uri", "mongodb://192.168.10.97:27017") \
                    .option("database", monthly_db) \
                    .option("collection", monthly_collection) \
                    .load()

                count = monthly_df.count()
                if count > 0:
                    print(f"Found {count} records in {monthly_collection}")
                    all_monthly_dfs.append(monthly_df.withColumn("month_num", lit(month)))
                else:
                    print(f"No data in {monthly_collection}")
            except Exception as e:
                print(f"Error reading {monthly_collection}: {str(e)}")

        if not all_monthly_dfs:
            print(f"No monthly data found for year {target_year}")
            return

        # 2. Union all monthly data
        if len(all_monthly_dfs) > 1:
            all_monthly_data = all_monthly_dfs[0]
            for df in all_monthly_dfs[1:]:
                all_monthly_data = all_monthly_data.union(df)
        else:
            all_monthly_data = all_monthly_dfs[0]

        print(f"Total monthly records: {all_monthly_data.count()}")

        # 3. Calculate the latest month for each phone number
        latest_month_window = Window.partitionBy("phone").orderBy(col("month_num").desc(), col("timestamp").desc())

        latest_membership = all_monthly_data \
            .withColumn("row_num", row_number().over(latest_month_window)) \
            .filter(col("row_num") == 1) \
            .select(
            "phone",
            "membershipcode",
            "membershipname"
        )

        # 4. Sum up total points by phone
        total_points = all_monthly_data.groupBy("phone") \
            .agg(
            sum("totalpoints").alias("yearly_total_points")
        )

        # 5. Join latest membership info with total points
        yearly_data = total_points.join(
            latest_membership,
            "phone",
            "inner"
        ).withColumn(
            "year",
            lit(target_year)
        ).withColumn(
            "timestamp",
            lit(int(datetime(target_year, 1, 1, tzinfo=pytz.timezone('Asia/Ho_Chi_Minh')).timestamp() * 1000))
        ).withColumn(
            "last_updated",
            current_timestamp()
        )

        # 6. Show sample results
        print("\nSample of recalculated yearly data:")
        yearly_data.select(
            "phone",
            "membershipcode",
            "membershipname",
            "yearly_total_points"
        ).orderBy(desc("yearly_total_points")).show(10, truncate=False)

        # 7. Check if yearly collection exists, if not create it
        db = mongo_client[yearly_db]
        collections = db.list_collection_names()
        collection_exists = yearly_collection in collections

        # 8. Prepare update operations
        records = yearly_data.collect()
        bulk_operations = []
        inserts = 0
        updates = 0

        # Get existing records if collection exists
        existing_phones = set()
        if collection_exists:
            for doc in db[yearly_collection].find({}, {"phone": 1}):
                if "phone" in doc:
                    existing_phones.add(doc["phone"])

        # Prepare operations
        for record in records:
            phone = record["phone"]

            doc = {
                "phone": phone,
                "year": target_year,
                "membershipcode": record["membershipcode"],
                "membershipname": record["membershipname"],
                "totalpoints": record["yearly_total_points"],
                "timestamp": record["timestamp"],
                "last_updated": datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
            }

            if phone in existing_phones:
                # Update existing record
                bulk_operations.append(
                    UpdateOne(
                        {"phone": phone, "year": target_year},
                        {"$set": doc},
                        upsert=False
                    )
                )
                updates += 1
            else:
                # Insert new record
                doc["created_at"] = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
                doc["rank"] = 0  # Default rank, will be updated by ranking job

                bulk_operations.append(
                    InsertOne(doc)
                )
                inserts += 1

        # 9. Execute bulk operations
        if bulk_operations:
            result = db[yearly_collection].bulk_write(bulk_operations, ordered=False)

            print(f"\nBulk operation results:")
            if hasattr(result, 'inserted_count'):
                print(f"  Inserted: {result.inserted_count}")
            if hasattr(result, 'modified_count'):
                print(f"  Modified: {result.modified_count}")
            if hasattr(result, 'matched_count'):
                print(f"  Matched: {result.matched_count}")

        print(f"\nRecalculation summary:")
        print(f"  Records to insert: {inserts}")
        print(f"  Records to update: {updates}")

        # 10. Create indexes
        db[yearly_collection].create_index([("phone", 1)], unique=True)
        db[yearly_collection].create_index([("rank", 1)])
        db[yearly_collection].create_index([("totalpoints", -1)])

        print(f"Successfully recalculated yearly data for {target_year}")

    except Exception as e:
        print(f"Error recalculating yearly data: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
        if mongo_client:
            mongo_client.close()


def main():
    """Main entry point of the script"""
    print(f"Starting Yearly Data Recalculation at {datetime.now()}")

    target_year = get_target_year()
    recalculate_yearly_data(target_year)

    print(f"Recalculation Completed at {datetime.now()}")


if __name__ == "__main__":
    main()