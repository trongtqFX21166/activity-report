#!/usr/bin/env python3
"""
UpdateMembershipForProfileYearly.py - Updates membership code and name in yearly collection
                                     using data from a specified month/year

This script reads membership data from a monthly collection in MongoDB and
updates the corresponding yearly collection with the membership codes and names.

Usage:
    spark-submit UpdateMembershipForProfileYearly.py --year 2025 --month 3

Parameters:
    --year or -y   : Source year to read membership data from
    --month or -m  : Source month to read membership data from

The script will update the current year's yearly collection with membership data
from the specified month/year.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pytz
import argparse
import sys
import logging
import os

# Set script name for better log identification
script_name = os.path.basename(__file__).replace('.py', '')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(script_name)


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("update_yearly_membership") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.read.database", "activity_membershiptransactionmonthly_dev") \
        .getOrCreate()


def get_mongodb_connection():
    """Get connection to MongoDB databases"""
    try:
        client = MongoClient('mongodb://192.168.10.97:27017/')
        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise


def parse_arguments():
    """Parse command line arguments for year and month with --year and --month format"""
    parser = argparse.ArgumentParser(description='Update Yearly Membership Codes from Monthly Data')
    parser.add_argument('--year', '-y', type=int, help='Year to read membership data from')
    parser.add_argument('--month', '-m', type=int, help='Month to read membership data from (1-12)')

    # Handle both direct arguments and spark-submit style (where script name is in sys.argv)
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        start_idx = sys.argv.index(__file__) + 1 if __file__ in sys.argv else 1
        args = parser.parse_args(sys.argv[start_idx:])
    else:
        args = parser.parse_args()

    # If arguments not provided, use current month/year
    if args.year is None or args.month is None:
        vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(vietnam_tz)

        if args.month is None:
            args.month = now.month
            logger.info(f"Month not specified, using current month: {args.month}")

        if args.year is None:
            args.year = now.year
            logger.info(f"Year not specified, using current year: {args.year}")

    # Validate month
    if not 1 <= args.month <= 12:
        logger.error(f"Invalid month: {args.month}. Month must be between 1 and 12.")
        sys.exit(1)

    logger.info(f"Processing data from month: {args.month}, year: {args.year}")
    return args.year, args.month


def get_current_year():
    """Get current year in Vietnam timezone"""
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)
    return now.year


def get_mongodb_membership_data(year, month):
    """Get membership data from MongoDB monthly collection using Spark"""
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


def update_yearly_membership(membership_data, current_year):
    """Update membership codes and names in yearly collection for the current year"""
    mongo_client = None
    try:
        # Initialize MongoDB client
        mongo_client = get_mongodb_connection()

        # Get the yearly DB and collection
        db = mongo_client['activity_membershiptransactionyearly_dev']
        collection_name = f"{current_year}"
        collection = db[collection_name]

        logger.info(f"Updating membership data in yearly collection: {collection_name}")

        # Prepare bulk update operations
        bulk_operations = []
        for member in membership_data:
            phone = member['phone']
            membershipcode = member['membershipcode']
            membershipname = member['membershipname']

            # Create update operation
            update_op = UpdateOne(
                {"phone": phone, "year": current_year},
                {"$set": {
                    "membershipcode": membershipcode,
                    "membershipname": membershipname,
                    "last_updated": datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
                }},
                upsert=False  # Don't create new documents, only update existing ones
            )
            bulk_operations.append(update_op)

        # Execute bulk update in batches
        if bulk_operations:
            batch_size = 500
            total_operations = len(bulk_operations)
            updated_count = 0
            matched_count = 0

            logger.info(f"Preparing to update {total_operations} records in batches of {batch_size}")

            for i in range(0, total_operations, batch_size):
                batch = bulk_operations[i:i + batch_size]
                result = collection.bulk_write(batch, ordered=False)
                updated_count += result.modified_count
                matched_count += result.matched_count

                logger.info(
                    f"Batch {i // batch_size + 1}: Matched {result.matched_count}, Modified {result.modified_count}")

            logger.info(f"Update summary: Matched {matched_count}, Updated {updated_count} records")
            return updated_count
        else:
            logger.warning("No operations to perform")
            return 0

    except Exception as e:
        logger.error(f"Error updating yearly membership: {str(e)}")
        raise
    finally:
        if mongo_client:
            mongo_client.close()


def main():
    """Main function to update yearly membership from monthly data"""
    start_time = datetime.now()
    logger.info(f"Starting yearly membership update process at {start_time}")
    logger.info(f"Script: {script_name}")

    try:
        # Parse arguments
        prev_year, prev_month = parse_arguments()

        # Get current year for yearly collection
        current_year = get_current_year()
        logger.info(f"Current year (target for updates): {current_year}")

        # Get membership data from the previous month
        membership_data = get_mongodb_membership_data(prev_year, prev_month)

        if not membership_data:
            logger.warning("No membership data to update. Exiting.")
            return

        # Update the yearly collection
        updated_count = update_yearly_membership(membership_data, current_year)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Yearly membership update process completed at {end_time}")
        logger.info(f"Updated {updated_count} records in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()