from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import calendar
import sys
import argparse
import os
from pymongo import MongoClient, UpdateOne, InsertOne
import pytz
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CloneMembershipToNextMonth")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Clone membership data from one month to the next')
    parser.add_argument('--source-month', type=int, help='Source month (1-12)')
    parser.add_argument('--source-year', type=int, help='Source year (e.g., 2024)')
    parser.add_argument('--target-month', type=int, help='Target month (1-12), defaults to next month')
    parser.add_argument('--target-year', type=int, help='Target year (e.g., 2024), defaults to appropriate year')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')

    # Handle positional arguments for backward compatibility
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        # Old style: script.py source_month source_year [target_month target_year]
        args = []
        if len(sys.argv) > 1:
            args.extend(['--source-month', sys.argv[1]])
        if len(sys.argv) > 2:
            args.extend(['--source-year', sys.argv[2]])
        if len(sys.argv) > 3:
            args.extend(['--target-month', sys.argv[3]])
        if len(sys.argv) > 4:
            args.extend(['--target-year', sys.argv[4]])

        # Parse with our custom args list
        args = parser.parse_args(args)
    else:
        # Parse normally with command line args
        args = parser.parse_args()

    # Check environment variables if environment not specified
    if not hasattr(args, 'env') or not args.env:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        if env in ['dev', 'prod']:
            args.env = env

    return args


def get_mongodb_config(env):
    """Return MongoDB configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': 'mongodb://192.168.10.97:27017',
            'database': 'activity_membershiptransactionmonthly_dev'
        }
    else:  # prod
        # Fixed the connection string format for production environment
        # Using proper format with authSource parameter in the URI itself
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017/?authSource=admin',
            'database': 'activity_membershiptransactionmonthly'
        }


def create_spark_session(env):
    """Create Spark session with MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    # Use the proper MongoDB connection URI format
    mongo_uri = mongo_config['host']
    database = mongo_config['database']

    # Build SparkSession with proper MongoDB connector configuration
    builder = SparkSession.builder \
        .appName(f"membership-monthly-cloner-{env}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1")

    # Configure MongoDB connection with connection.uri which is the correct property
    builder = builder.config("spark.mongodb.connection.uri", mongo_uri)

    # Create and return the SparkSession
    spark = builder.getOrCreate()
    logger.info(f"Created SparkSession with app ID: {spark.sparkContext.applicationId}")
    logger.info(f"Connected to MongoDB: {mongo_uri.split('@')[-1]} database: {database}")

    return spark


def get_mongodb_connection(env):
    """Create MongoDB client connection"""
    try:
        mongo_config = get_mongodb_config(env)

        # Use the URI directly which includes authentication if needed
        client = MongoClient(mongo_config['host'])

        # Test connection
        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB ({env} environment)")
        return client, mongo_config['database']
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
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
            raise ValueError(f"Month must be between 1 and 12, got {month}")

        # Create date object to validate year
        datetime(year, month, 1)
        return True
    except ValueError as e:
        logger.error(f"Invalid date: {str(e)}")
        return False


def clone_monthly_data(source_month, source_year, target_month=None, target_year=None, env='dev'):
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

        logger.info(
            f"Cloning data from {source_month}/{source_year} to {target_month}/{target_year} in {env} environment")

        # Initialize connections
        spark = create_spark_session(env)
        mongo_client, db_name = get_mongodb_connection(env)
        db = mongo_client[db_name]

        # Define collection names
        source_collection = f"{source_year}_{source_month}"
        target_collection = f"{target_year}_{target_month}"

        # Get MongoDB configuration
        mongo_config = get_mongodb_config(env)

        # Log source collection details
        logger.info(f"Reading from source collection: {source_collection}")
        logger.info(f"Database: {db_name}")

        # Read source month data with improved configuration
        source_data = spark.read \
            .format("mongodb") \
            .option("database", db_name) \
            .option("collection", source_collection) \
            .load()

        # Check if source data exists
        source_count = source_data.count()
        if source_count == 0:
            logger.warning(f"No data found for source month {source_month}/{source_year}")
            return

        logger.info(f"Found {source_count} records in source month")

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
                    logger.info(
                        f"Processed batch {i // batch_size + 1}/{(len(bulk_operations) + batch_size - 1) // batch_size}: "
                        f"Upserted: {result.upserted_count}, Modified: {result.modified_count}, "
                        f"Matched: {result.matched_count}")
                except Exception as e:
                    logger.error(f"Error processing batch {i // batch_size + 1}: {str(e)}")
                    # Continue with next batch instead of failing entire process

            logger.info(f"\nBulk operation results:")
            logger.info(f"  Upserted: {total_upserted}")
            logger.info(f"  Modified: {total_modified}")
            logger.info(f"  Matched: {total_matched}")
            logger.info(f"  Total processed: {total_upserted + total_modified}")

        # Create indexes on the collection
        target_coll = db[target_collection]
        target_coll.create_index([("phone", 1)], unique=True)
        target_coll.create_index([("rank", 1)])
        target_coll.create_index([("totalpoints", -1)])

        logger.info(f"Successfully cloned data to {target_month}/{target_year}")

        # Show sample of cloned data
        logger.info("\nSample of cloned records:")
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

        return True

    except Exception as e:
        logger.error(f"Error cloning monthly data: {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()
        if mongo_client:
            mongo_client.close()


def main():
    start_time = datetime.now()
    logger.info(f"Starting Membership Monthly Clone Process at {start_time}")

    try:
        # Parse arguments
        args = parse_arguments()

        # Validate that we have source month/year
        if not args.source_month or not args.source_year:
            logger.error("Source month and year are required")
            sys.exit(1)

        # Log environment
        logger.info(f"Running in {args.env.upper()} environment")

        # Clone data
        success = clone_monthly_data(
            args.source_month,
            args.source_year,
            args.target_month,
            args.target_year,
            args.env
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if success:
            logger.info(f"Clone Process Completed Successfully at {end_time}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            sys.exit(0)
        else:
            logger.error(f"Clone Process Failed at {end_time}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()