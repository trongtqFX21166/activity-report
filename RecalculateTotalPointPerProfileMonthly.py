from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pytz
import sys


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("activity_batch_points_summary") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()


def get_date_params():
    """Get month and year for processing, either from arguments or current date"""
    if len(sys.argv) >= 3:
        # If provided as arguments
        return int(sys.argv[1]), int(sys.argv[2])
    else:
        # Use current month/year
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


def process_activity_points(month, year):
    """Process activity transaction data and update MongoDB"""
    spark = None
    mongo_client = None

    try:
        # Initialize Spark
        spark = create_spark_session()
        print(f"Processing activity points for {month}/{year}")

        # Read activity transactions from Delta table
        transactions_df = spark.read.format("delta").table("activity_dev.activity_transaction")

        # Filter relevant data - only AddPoint transactions for the specified month and year
        filtered_df = transactions_df.filter(
            (col("Month") == month) &
            (col("Year") == year) &
            (col("Name") == "AddPoint")
        )

        # Count records for logging
        filtered_count = filtered_df.count()
        print(f"Found {filtered_count} AddPoint transactions for {month}/{year}")

        if filtered_count == 0:
            print("No transactions to process")
            return

        # Aggregate points by phone, month, year
        points_summary = filtered_df.groupBy("Phone", "Month", "Year", "MembershipCode") \
            .agg(
            sum("Value").alias("points"),
            max("TimeStamp").alias("timestamp")
        )

        # Show summary for logging
        print("\nPoints summary:")
        points_summary.show(10, truncate=False)

        # Initialize MongoDB connection
        mongo_client = get_mongodb_connection()
        db = mongo_client['activity_membershiptransactionmonthly_dev']
        collection_name = f"{year}_{month}"
        collection = db[collection_name]

        # Prepare batch updates
        updates = points_summary.collect()
        bulk_operations = []

        for row in updates:
            # Create update operation for MongoDB
            update_op = UpdateOne(
                {
                    "phone": row["Phone"],
                    "month": row["Month"],
                    "year": row["Year"]
                },
                {
                    "$set": {
                        "totalpoints": row["points"],
                        "timestamp": row["timestamp"],
                        "last_updated": datetime.now()
                    },
                    "$setOnInsert": {
                        "rank": 9999,  # Default rank, will be updated by ranking job
                        "created_at": datetime.now()
                    }
                },
                upsert=True
            )
            bulk_operations.append(update_op)

        # Execute bulk update
        if bulk_operations:
            result = collection.bulk_write(bulk_operations, ordered=False)
            print(f"\nMongoDB Update Results:")
            print(f"  Matched: {result.matched_count}")
            print(f"  Modified: {result.modified_count}")
            print(f"  Upserted: {result.upserted_count}")

        # Check if we should trigger ranking job
        # In production, this would likely be a separate DAG or task in Airflow

    except Exception as e:
        print(f"Error processing activity points: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
        if mongo_client:
            mongo_client.close()


def main():
    """Main entry point of the script"""
    print(f"Starting Activity Batch Points Processing at {datetime.now()}")

    # Get month and year to process
    month, year = get_date_params()

    # Process activity points
    process_activity_points(month, year)

    print(f"Activity Batch Points Processing Completed at {datetime.now()}")


if __name__ == "__main__":
    main()