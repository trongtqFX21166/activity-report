from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import pytz
import argparse
import sys
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("UpdateMembershipCode")


def parse_arguments():
    """Parse command line arguments for year and month"""
    parser = argparse.ArgumentParser(description='Update Profile Membership Codes')
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--month', type=int, help='Month to process')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')

    # Parse known args to handle cases where additional arguments exist
    args, _ = parser.parse_known_args()

    # If environment not specified in args, check environment variables
    if not args.env:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        if env in ['dev', 'prod']:
            args.env = env

    # If arguments not provided, use current month/year
    if args.year is None or args.month is None:
        vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(vietnam_tz)
        args.month = args.month or now.month
        args.year = args.year or now.year

    # Validate month
    if not 1 <= args.month <= 12:
        logger.error(f"Invalid month: {args.month}. Month must be between 1 and 12.")
        sys.exit(1)

    logger.info(f"Processing data for month: {args.month}, year: {args.year}, environment: {args.env}")
    return args


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


def get_postgres_config(env):
    """Return PostgreSQL configuration for the specified environment"""
    if env == 'dev':
        return {
            "host": "192.168.8.230",
            "database": "TrongTestDB1",
            "user": "postgres",
            "password": "admin123."
        }
    else:  # prod
        return {
            "host": "192.168.11.83",
            "database": "ActivityDB",
            "user": "vmladmin",
            "password": "5d6v6hiFGGns4onnZGW0VfKe"
        }

def create_spark_session(app_name, env):
    """Create Spark session with MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    # Use the proper MongoDB connection URI format
    mongo_uri = mongo_config['host']
    database = mongo_config['database']

    # Build SparkSession with proper MongoDB connector configuration
    builder = SparkSession.builder \
        .appName(f"{app_name}-{env}") \
        .config("spark.mongodb.database", database) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1")

    # Configure MongoDB connection with connection.uri which is the correct property
    builder = builder.config("spark.mongodb.connection.uri", mongo_uri)

    # Create and return the SparkSession
    spark = builder.getOrCreate()
    logger.info(f"Created SparkSession with app ID: {spark.sparkContext.applicationId}")
    logger.info(f"Connected to MongoDB: {mongo_uri.split('@')[-1]} database: {database}")

    return spark


def get_postgres_connection(env):
    """Get connection to PostgreSQL database"""
    postgres_config = get_postgres_config(env)
    try:
        conn = psycopg2.connect(**postgres_config)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise


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


def update_postgres_membership_codes(membership_data, env, batch_size=1000):
    """Update membership codes in PostgreSQL profiles table"""
    # Prepare the update SQL
    update_query = """
        UPDATE public."Profiles"
        SET "MembershipCode" = %s,
            "LastModified" = %s,
            "LastModifiedBy" = %s
        WHERE "Phone" = %s
    """

    current_time = datetime.now()
    modified_by = "MembershipCodeUpdater"

    conn = None
    try:
        conn = get_postgres_connection(env)
        with conn.cursor() as cur:
            # Prepare batch data
            batch_data = [
                (
                    row['membershipcode'],
                    current_time,
                    modified_by,
                    row['phone']
                )
                for row in membership_data
            ]

            # Execute batch update
            execute_batch(cur, update_query, batch_data, page_size=batch_size)
            conn.commit()

            logger.info(f"Successfully updated {len(batch_data)} profile membership codes")
            return len(batch_data)
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error updating PostgreSQL profiles: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def get_mongodb_membership_data(year, month, env):
    """Get membership data from MongoDB using Spark"""
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session("update_profile_membership", env)

        # Collection name based on year and month
        collection_name = f"{year}_{month}"
        logger.info(f"Reading data from MongoDB collection: {collection_name}")

        # Read from MongoDB
        df = spark.read \
            .format("mongodb") \
            .option("database", 'activity_membershiptransactionmonthly') \
            .option("collection", collection_name) \
            .option("readConcern.level", "majority") \
            .load()

        # Check if we have data
        count = df.count()
        if count == 0:
            logger.warning(f"No data found for {month}/{year}")
            return []

        logger.info(f"Found {count} records in MongoDB collection")

        # Select only needed columns and convert to lowercase
        membership_df = df.select(
            col("phone"),
            col("membershipcode"),
            col("membershipname"),
            col("rank"),
            col("totalpoints")
        )

        # Sort by rank to get the best members first
        membership_df = membership_df.orderBy("rank")

        # Convert to Python objects
        membership_data = [row.asDict() for row in membership_df.collect()]

        # Show sample for logging
        logger.info("Sample membership data (top 5 by rank):")
        for i, row in enumerate(membership_data[:5]):
            logger.info(f"  {i + 1}. Phone: {row['phone']}, Code: {row['membershipcode']}, "
                        f"Name: {row['membershipname']}, Rank: {row['rank']}")

        return membership_data

    except Exception as e:
        logger.error(f"Error reading from MongoDB: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


def main():
    """Main function to update profile membership codes"""
    start_time = datetime.now()
    logger.info(f"Starting membership code update process at {start_time}")

    try:
        # Parse arguments
        args = parse_arguments()

        # Validate month and year
        if not validate_month_year(args.month, args.year):
            logger.error(f"Invalid month/year combination: {args.month}/{args.year}")
            sys.exit(1)

        # Get membership data using PySpark
        logger.info(f"Using Spark to read MongoDB data from {args.env} environment")
        membership_data = get_mongodb_membership_data(args.year, args.month, args.env)

        if not membership_data:
            logger.warning("No membership data to update. Exiting.")
            return

        # Update PostgreSQL profiles
        updated_count = update_postgres_membership_codes(membership_data, args.env)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Membership code update process completed at {end_time}")
        logger.info(f"Updated {updated_count} profiles in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()