from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import calendar
import sys
from pymongo import MongoClient, UpdateOne, InsertOne
import pytz


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("membership-monthly-cloner") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.write.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.read.database", "activity_membershiptransactionmonthly_dev") \
        .config("spark.mongodb.write.database", "activity_membershiptransactionmonthly_dev") \
        .getOrCreate()


def get_mongodb_connection():
    """Create MongoDB client connection"""
    try:
        client = MongoClient('mongodb://192.168.10.97:27017/')
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        raise


def get_next_month_year(current_month, current_year):
    """Calculate next month and year"""
    if current_month == 12:
        return 1, current_year + 1
    return current_month + 1, current_year


def validate_date(month, year):
    """Validate if the provided month and year are valid"""
    try:
        if not (1 <= month <= 12):
            raise ValueError("Month must be between 1 and 12")

        # Create date object to validate year
        datetime(year, month, 1)
        return True
    except ValueError as e:
        print(f"Invalid date: {str(e)}")
        return False


def clone_monthly_data(source_month, source_year, target_month=None, target_year=None):
    """Clone membership transaction data from source month to target month"""
    spark = None
    mongo_client = None

    try:
        # Validate source date
        if not validate_date(source_month, source_year):
            raise ValueError(f"Invalid source date: month={source_month}, year={source_year}")

        # If target month/year not provided, use next month
        if target_month is None or target_year is None:
            target_month, target_year = get_next_month_year(source_month, source_year)

        # Validate target date
        if not validate_date(target_month, target_year):
            raise ValueError(f"Invalid target date: month={target_month}, year={target_year}")

        print(f"Cloning data from {source_month}/{source_year} to {target_month}/{target_year}")

        # Initialize connections
        spark = create_spark_session()
        mongo_client = get_mongodb_connection()
        db = mongo_client['activity_membershiptransactionmonthly_dev']

        # Define collection names
        source_collection = f"{source_year}_{source_month}"
        target_collection = f"{target_year}_{target_month}"

        # Read source month data
        source_data = spark.read \
            .format("mongodb") \
            .option("collection", source_collection) \
            .load()

        # Check if source data exists
        source_count = source_data.count()
        if source_count == 0:
            print(f"No data found for source month {source_month}/{source_year}")
            return

        print(f"Found {source_count} records in source month")

        # Calculate timestamp for first day of target month
        target_timestamp = int(
            datetime(target_year, target_month, 1, tzinfo=pytz.timezone('Asia/Ho_Chi_Minh')).timestamp() * 1000)

        # Prepare data for target month
        target_data = source_data \
            .drop("_id") \
            .withColumn("month", lit(target_month)) \
            .withColumn("year", lit(target_year)) \
            .withColumn("timestamp", lit(target_timestamp)) \
            .withColumn("totalpoints", lit(0)) \
            .withColumn("last_updated", current_timestamp())

        # Convert to list of dictionaries for MongoDB operations
        records = target_data.collect()

        # Prepare bulk operations - use UpdateOne with upsert=True for all operations
        bulk_operations = []
        for record in records:
            doc = record.asDict()
            phone = doc.get("phone")

            # Use UpdateOne with upsert=True for both new and existing records
            bulk_operations.append(
                UpdateOne(
                    {"phone": phone, "month": target_month, "year": target_year},
                    {"$set": doc},
                    upsert=True
                )
            )

        # Execute bulk operations if any
        if bulk_operations:
            collection = db[target_collection]

            # Use batch processing to avoid overwhelming MongoDB
            batch_size = 1000
            total_upserted = 0
            total_modified = 0
            total_matched = 0

            for i in range(0, len(bulk_operations), batch_size):
                batch = bulk_operations[i:i + batch_size]
                try:
                    result = collection.bulk_write(batch, ordered=False)
                    total_upserted += result.upserted_count
                    total_modified += result.modified_count
                    total_matched += result.matched_count
                    print(
                        f"Processed batch {i // batch_size + 1}/{(len(bulk_operations) + batch_size - 1) // batch_size}: "
                        f"Upserted: {result.upserted_count}, Modified: {result.modified_count}, "
                        f"Matched: {result.matched_count}")
                except Exception as e:
                    print(f"Error processing batch {i // batch_size + 1}: {str(e)}")
                    # Continue with next batch instead of failing entire process

            print(f"\nBulk operation results:")
            print(f"  Upserted: {total_upserted}")
            print(f"  Modified: {total_modified}")
            print(f"  Matched: {total_matched}")
            print(f"  Total processed: {total_upserted + total_modified}")

        # Create indexes on the collection
        target_coll = db[target_collection]
        target_coll.create_index([("phone", 1)], unique=True)
        target_coll.create_index([("rank", 1)])
        target_coll.create_index([("totalpoints", -1)])

        print(f"Successfully cloned data to {target_month}/{target_year}")

        # Show sample of cloned data
        print("\nSample of cloned records:")
        target_data.select(
            "phone",
            "membershipcode",
            "membershipname",
            "month",
            "year",
            "rank",
            "totalpoints",
            "timestamp"
        ).show(5, truncate=False)

    except Exception as e:
        print(f"Error cloning monthly data: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
        if mongo_client:
            mongo_client.close()


def main():
    # Check command line arguments
    if len(sys.argv) < 3:
        print("Usage: script.py <source_month> <source_year> [target_month] [target_year]")
        sys.exit(1)

    source_month = int(sys.argv[1])
    source_year = int(sys.argv[2])

    target_month = int(sys.argv[3]) if len(sys.argv) > 3 else None
    target_year = int(sys.argv[4]) if len(sys.argv) > 4 else None

    print(f"Starting Membership Monthly Clone Process at {datetime.now()}")
    clone_monthly_data(source_month, source_year, target_month, target_year)
    print(f"Clone Process Completed at {datetime.now()}")


if __name__ == "__main__":
    main()