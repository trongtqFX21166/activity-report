from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pymongo import MongoClient, UpdateOne
import pytz


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("membershiptransactionmonthly_ranking") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.write.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.read.database", "activity_membershiptransactionyearly_dev") \
        .config("spark.mongodb.write.database", "activity_membershiptransactionyearly_dev") \
        .getOrCreate()


def get_current_month_year():
    """Get current month and year in Vietnam timezone"""
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)
    return now.month, now.year


def get_mongodb_connection():
    """Create MongoDB client connection"""
    try:
        client = MongoClient('mongodb://192.168.10.97:27017/')
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        raise


def calculate_unique_ranks():
    """Calculate and update unique ranks for the current month"""
    spark = None
    mongo_client = None

    try:
        # Initialize connections
        spark = create_spark_session()
        mongo_client = get_mongodb_connection()
        db = mongo_client['activity_membershiptransactionyearly_dev']

        # Get current month/year
        current_month, current_year = get_current_month_year()
        collection_name = f"{current_year}"
        print(f"Processing rankings for collection: {collection_name}")

        # Read current month's data from MongoDB
        df = spark.read \
            .format("mongodb") \
            .option("collection", collection_name) \
            .option("readConcern.level", "majority") \
            .load()

        if df.count() == 0:
            print(f"No data found for year {current_year}")
            return

        # Create window spec for ranking
        window_spec = Window.orderBy(
            desc("totalpoints"),
            asc("timestamp"),
            asc("phone")
        )

        # Calculate unique ranks
        ranked_df = df.withColumn("new_rank", row_number().over(window_spec))

        # Prepare updates
        updates = ranked_df.select(
            col("_id"),
            col("phone"),
            col("membershipcode"),
            col("membershipname"),
            col("year"),
            col("new_rank").alias("rank"),
            col("totalpoints"),
            col("timestamp")
        ).collect()

        # Prepare bulk updates for MongoDB
        collection = db[collection_name]
        bulk_operations = []

        for row in updates:
            update_operation = UpdateOne(
                {
                    "phone": row["phone"]
                 },
                {
                    "$set": {
                        "rank": row["rank"],
                        "last_updated": datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')),
                        "phone": row["phone"],
                        "year": row["year"],
                        "timestamp": row["timestamp"]
                    }
                },
                upsert=False
            )
            bulk_operations.append(update_operation)

        # Execute bulk update
        if bulk_operations:
            result = collection.bulk_write(bulk_operations, ordered=False)
            print(f"MongoDB Update Results - Modified: {result.modified_count}, "
                  f"Upserted: {result.upserted_count}, Matched: {result.matched_count}")

        # Log ranking distribution
        print("\nRanking Distribution:")
        ranked_df.select(
            "new_rank",
            "totalpoints",
            "phone",
            "timestamp"
        ).orderBy("new_rank").show(10)

        # Verify no duplicate ranks
        duplicate_ranks = ranked_df.groupBy("new_rank").count().filter(col("count") > 1)
        if duplicate_ranks.count() > 0:
            print("Warning: Duplicate ranks found!")
            duplicate_ranks.show()
        else:
            print("Success: All ranks are unique!")

        # Show examples of tied points
        print("\nExamples of Users with Same Points but Different Ranks:")
        window_points = Window.partitionBy("totalpoints")
        tied_points_df = ranked_df \
            .withColumn("users_with_same_points", count("*").over(window_points)) \
            .where(col("users_with_same_points") > 1) \
            .orderBy("totalpoints", "new_rank") \
            .select(
            "totalpoints",
            "new_rank",
            "phone",
            "timestamp"
        )
        tied_points_df.show(10)

        # Show points distribution
        print("\nPoints Distribution Summary:")
        ranked_df.summary("count", "min", "max", "mean").select(
            "summary",
            "totalpoints"
        ).show()

    except Exception as e:
        print(f"Error calculating unique ranks: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
        if mongo_client:
            mongo_client.close()


def main():
    """Main entry point of the script"""
    print(f"Starting MembershipTransactionMonthly Ranking Process at {datetime.now()}")
    calculate_unique_ranks()
    print(f"MembershipTransactionMonthly Ranking Process Completed at {datetime.now()}")


if __name__ == "__main__":
    main()