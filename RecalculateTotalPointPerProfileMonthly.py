from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pytz
import sys
import os
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RecalculateMonthlyPoints")


def get_environment():
    """
    Determine the execution environment (dev or prod).
    Can be specified as a command-line argument or environment variable.
    Defaults to 'dev' if not specified.
    """
    parser = argparse.ArgumentParser(description='Process monthly points')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')
    parser.add_argument('--month', type=int, help='Month to process')
    parser.add_argument('--year', type=int, help='Year to process')

    # Parse known args to avoid conflicts with other args that might be passed
    args, _ = parser.parse_known_args()

    # Check environment variables if not specified in args
    if not args.env:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        if env in ['dev', 'prod']:
            return env, args.month, args.year

    return args.env, args.month, args.year


def get_mongodb_config(env):
    """Return MongoDB configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': 'mongodb://192.168.10.97:27017',
            'database': 'activity_membershiptransactionmonthly_dev'
        }
    else:  # prod
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017',
            'database': 'activity_membershiptransactionmonthly',
            'auth_source': 'admin'
        }


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


def get_date_params(month=None, year=None):
    """Get month and year for processing, either from arguments or current date"""
    if month is not None and year is not None:
        return month, year

    # Use current month/year
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)
    return now.month, now.year


def validate_month_year(month, year):
    """Validate month and year parameters"""
    try:
        if not (1 <= month <= 12):
            raise ValueError(f"Month must be between 1 and 12, got {month}")

        # Create a date object to validate the date
        datetime(year, month, 1)
        return True
    except ValueError as e:
        logger.error(f"Invalid month/year: {str(e)}")
        return False


def get_mongodb_connection(env):
    """Create MongoDB client connection"""
    try:
        mongo_config = get_mongodb_config(env)

        # Create client with appropriate authentication
        if env == 'dev':
            client = MongoClient(mongo_config['host'])
        else:  # prod
            client = MongoClient(
                mongo_config['host'],
                authSource=mongo_config['auth_source']
            )

        # Test connection
        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB ({env} environment)")
        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise


def process_activity_points(month, year, env):
    """Process activity transaction data and update MongoDB"""
    spark = None
    mongo_client = None

    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info(f"Processing activity points for {month}/{year} in {env} environment")

        # Validate the month and year
        if not validate_month_year(month, year):
            logger.error(f"Invalid month/year: {month}/{year}")
            return False

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
        logger.info(f"Found {filtered_count} AddPoint transactions for {month}/{year}")

        if filtered_count == 0:
            logger.info("No transactions to process")
            return False

        # Aggregate points by phone, month, year
        points_summary = filtered_df.groupBy("Phone", "Month", "Year", "MembershipCode") \
            .agg(
            sum("Value").alias("points"),
            max("TimeStamp").alias("timestamp")
        )

        # Show summary for logging
        logger.info("\nPoints summary:")
        points_summary.show(10, truncate=False)

        # Initialize MongoDB connection
        mongo_client = get_mongodb_connection(env)
        mongo_config = get_mongodb_config(env)
        db = mongo_client[mongo_config['database']]
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
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                logger.info(f"\nMongoDB Update Results:")
                logger.info(f"  Matched: {result.matched_count}")
                logger.info(f"  Modified: {result.modified_count}")
                logger.info(f"  Upserted: {result.upserted_count}")

                # Create indices if they don't exist
                collection.create_index([("phone", 1)], unique=True)
                collection.create_index([("rank", 1)])
                collection.create_index([("totalpoints", -1)])

                return True
            except Exception as e:
                logger.error(f"Error updating MongoDB: {str(e)}")
                return False

        return False

    except Exception as e:
        logger.error(f"Error processing activity points: {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()
        if mongo_client:
            mongo_client.close()


def main():
    """Main entry point of the script"""
    start_time = datetime.now()
    logger.info(f"Starting Activity Batch Points Processing at {start_time}")

    # Get environment and processing parameters
    env, cmd_month, cmd_year = get_environment()

    # Get month and year to process
    month, year = get_date_params(cmd_month, cmd_year)

    logger.info(f"Environment: {env.upper()}")
    logger.info(f"Processing data for {month}/{year}")

    # Process activity points
    success = process_activity_points(month, year, env)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    if success:
        logger.info(f"Activity Batch Points Processing Completed Successfully at {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")
    else:
        logger.info(f"Activity Batch Points Processing Failed or No Updates Required at {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")


if __name__ == "__main__":
    main()