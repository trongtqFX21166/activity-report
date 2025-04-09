from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pymongo import MongoClient, UpdateOne
import os
import sys
import argparse
from datetime import datetime
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
logger = logging.getLogger("MembershipProfile")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process membership ranking and update membership codes')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2024)')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')

    # Parse known args to handle cases where additional arguments exist
    args, _ = parser.parse_known_args()

    # If environment not specified in args, check environment variables
    if not args.env:
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
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017',
            'database': 'activity_membershiptransactionmonthly',
            'auth_source': 'admin'
        }


def create_spark_session(app_name, env):
    """Create Spark session with MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.read.connection.uri", f"{mongo_config['host']}/{mongo_config['database']}") \
        .config("spark.mongodb.write.connection.uri", f"{mongo_config['host']}/{mongo_config['database']}") \
        .config("spark.mongodb.read.database", mongo_config['database']) \
        .config("spark.mongodb.write.database", mongo_config['database'])

    # Add authentication for production
    if env == 'prod' and 'auth_source' in mongo_config:
        spark = spark \
            .config("spark.mongodb.auth.source", mongo_config['auth_source'])

    return spark.enableHiveSupport().getOrCreate()


def get_membership_mapping():
    """Define membership levels based on rank percentiles"""
    return [
        (0.00, 0.01, "Level4", "Nổi tiếng"),  # Top 1%
        (0.01, 0.05, "Level3", "Uy tín"),  # Next 4%
        (0.05, 0.15, "Level2", "Ngôi sao đang lên"),  # Next 10%
        (0.15, 1.00, "Level1", "Nhập môn")  # Remaining 85%
    ]


def get_default_month_year():
    """Get current month and year in Vietnam timezone"""
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)
    return now.month, now.year


def validate_month_year(month, year):
    """Validate month and year parameters"""
    try:
        if not (1 <= month <= 12):
            raise ValueError(f"Month must be between 1 and 12, got {month}")

        # Create a date object to validate year (will raise ValueError for invalid year)
        datetime(year, month, 1)
        return True
    except ValueError as e:
        logger.error(f"Error validating month/year: {str(e)}")
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


def assign_membership_levels(df):
    """Assign membership levels based on rank percentiles"""
    # Calculate total number of users
    total_users = df.count()

    if total_users == 0:
        logger.info("No users found in the dataset")
        return df

    logger.info(f"Total users for selected period: {total_users}")

    # Calculate rank first
    window_spec = Window.orderBy(desc("totalpoints"))
    df = df.withColumn("rank", row_number().over(window_spec))

    # Calculate rank percentile for each user
    ranked_df = df \
        .withColumn("percentile", col("rank") / total_users)

    # Apply membership rules based on percentiles
    mappings = get_membership_mapping()
    membership_conditions = []

    for min_perc, max_perc, code, name in mappings:
        condition = (
            when((col("percentile") > min_perc) & (col("percentile") <= max_perc),
                 struct(lit(code).alias("code"), lit(name).alias("name")))
        )
        membership_conditions.append(condition)

    membership_expr = coalesce(*membership_conditions)

    return ranked_df \
        .withColumn("membership", membership_expr) \
        .withColumn("membershipCode", col("membership.code")) \
        .withColumn("membershipName", col("membership.name")) \
        .drop("membership")


def update_membership_status(month=None, year=None, env='dev'):
    """Update membership status for specified month and year"""
    spark = None
    client = None

    try:
        # Get default month and year if not specified
        if month is None or year is None:
            month, year = get_default_month_year()

        # Validate month and year
        if not validate_month_year(month, year):
            logger.error(f"Invalid month/year combination: {month}/{year}")
            return False

        logger.info(f"Processing data for month: {month}, year: {year}, environment: {env}")

        # Initialize Spark
        app_name = f"membership_rank_updater_{year}_{month}_{env}"
        spark = create_spark_session(app_name, env)

        # Initialize MongoDB client
        client = get_mongodb_connection(env)
        mongo_config = get_mongodb_config(env)
        db = client[mongo_config['database']]
        collection_name = f"{year}_{month}"
        collection = db[collection_name]

        # Read specified month's data from MongoDB
        monthly_data = spark.read \
            .format("mongodb") \
            .option("collection", collection_name) \
            .load()

        # Check if we have data for specified month
        record_count = monthly_data.count()
        if record_count == 0:
            logger.warning(f"No data found for month {month}, year {year}")
            return False

        logger.info(f"Found {record_count} records to process")

        # Calculate and apply new membership levels
        membership_updates = assign_membership_levels(monthly_data)

        # Prepare final updates
        final_updates = membership_updates.select(
            col("phone"),
            col("membershipcode"),
            col("membershipname"),
            lit(month).alias("month"),
            lit(year).alias("year"),
            col("rank"),
            col("totalpoints"),
            col("timestamp"),
            current_timestamp().alias("processed_timestamp")
        )

        # Convert DataFrame to list of dictionaries for MongoDB
        updates = final_updates.collect()
        bulk_operations = []

        for update in updates:
            # Create update operation
            doc = update.asDict()
            filter_doc = {
                'phone': doc['phone'],
                'month': doc['month'],
                'year': doc['year']
            }
            update_doc = {'$set': doc}

            bulk_operations.append(
                UpdateOne(
                    filter_doc,
                    update_doc,
                    upsert=True
                )
            )

        # Create indices to ensure performance
        try:
            collection.create_index([("phone", 1)], unique=True)
            collection.create_index([("rank", 1)])
            collection.create_index([("totalpoints", -1)])
        except Exception as e:
            logger.warning(f"Index creation note: {str(e)}")

        # Execute bulk update
        if bulk_operations:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                logger.info(
                    f"MongoDB Update Results - Modified: {result.modified_count}, Upserted: {result.upserted_count}")
            except Exception as e:
                if "duplicate key error" in str(e):
                    logger.warning("Warning: Duplicate phone numbers detected. Handling duplicates...")
                    # Handle duplicates by keeping the record with the highest points
                    deduped_updates = final_updates.dropDuplicates(["phone"], "totalpoints")

                    # Create new bulk operations with deduplicated data
                    bulk_operations = []
                    for update in deduped_updates.collect():
                        doc = update.asDict()
                        bulk_operations.append(
                            UpdateOne(
                                {'phone': doc['phone']},
                                {'$set': doc},
                                upsert=True
                            )
                        )

                    # Retry bulk write with deduplicated data
                    result = collection.bulk_write(bulk_operations, ordered=False)
                    logger.info(
                        f"After deduplication - Modified: {result.modified_count}, Upserted: {result.upserted_count}")
                else:
                    logger.error(f"Error in bulk write: {str(e)}")
                    raise

        # Show statistics
        logger.info("\nMembership Level Distribution:")
        final_updates.groupBy("membershipCode", "membershipName") \
            .agg(
            count("*").alias("user_count"),
            round(avg("totalpoints"), 2).alias("avg_points"),
            round(min("totalpoints"), 2).alias("min_points"),
            round(max("totalpoints"), 2).alias("max_points")
        ) \
            .orderBy("membershipCode") \
            .show(truncate=False)

        # Show sample updates
        logger.info("\nSample Records (sorted by points):")
        final_updates.orderBy(desc("totalpoints")) \
            .select(
            "phone",
            "membershipCode",
            "membershipName",
            "rank",
            "totalpoints"
        ) \
            .show(10, truncate=False)

        return True

    except Exception as e:
        logger.error(f"Error updating membership status: {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()
        if client:
            client.close()


def main():
    """Main entry point of the script"""
    start_time = datetime.now()

    try:
        # Parse command line arguments
        args = parse_arguments()

        # If no month/year provided, use current
        month = args.month
        year = args.year
        env = args.env

        if month is None or year is None:
            curr_month, curr_year = get_default_month_year()
            month = month or curr_month
            year = year or curr_year

        logger.info(
            f"Starting membership status update process for {month}/{year} in {env} environment at {start_time}")

        # Process membership update
        success = update_membership_status(month, year, env)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if success:
            logger.info(f"Membership status update process completed successfully for {month}/{year}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            sys.exit(0)
        else:
            logger.error(f"Membership status update process failed for {month}/{year}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()