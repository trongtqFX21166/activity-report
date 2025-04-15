from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import pytz
import argparse
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("UpdateMembershipCode")


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("update_profile_membership_code") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.read.database", "activity_membershiptransactionmonthly_dev") \
        .getOrCreate()


def get_postgres_connection():
    """Get connection to PostgreSQL database"""
    return psycopg2.connect(
        host="192.168.8.230",
        database="TrongTestDB1",
        user="postgres",
        password="admin123."
    )


def parse_arguments():
    """Parse command line arguments for year and month"""
    parser = argparse.ArgumentParser(description='Update Profile Membership Codes')
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--month', type=int, help='Month to process')

    args = parser.parse_args()

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

    logger.info(f"Processing data for month: {args.month}, year: {args.year}")
    return args.year, args.month


def update_postgres_membership_codes(membership_data, batch_size=1000):
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
        conn = get_postgres_connection()
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


def get_mongodb_membership_data(year, month):
    """Get membership data from MongoDB using Spark"""
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()

        # Collection name based on year and month
        collection_name = f"{year}_{month}"
        logger.info(f"Reading data from MongoDB collection: {collection_name}")

        # Read from MongoDB
        df = spark.read \
            .format("mongodb") \
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
        year, month = parse_arguments()

        # Get membership data using PySpark
        logger.info("Using Spark to read MongoDB data")
        membership_data = get_mongodb_membership_data(year, month)

        if not membership_data:
            logger.warning("No membership data to update. Exiting.")
            return

        # Update PostgreSQL profiles
        updated_count = update_postgres_membership_codes(membership_data)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Membership code update process completed at {end_time}")
        logger.info(f"Updated {updated_count} profiles in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()