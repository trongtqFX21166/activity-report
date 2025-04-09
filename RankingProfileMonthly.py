from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pymongo import MongoClient, UpdateOne
import pytz
import argparse
import sys
import os


def get_environment():
    """
    Determine the execution environment (dev or prod).
    Can be specified as a command-line argument or environment variable.
    Defaults to 'dev' if not specified.
    """
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() in ['dev', 'prod']:
        return sys.argv[1].lower()

    # Check environment variables
    env = os.environ.get('ENVIRONMENT', 'dev').lower()
    if env in ['dev', 'prod']:
        return env

    # Default to dev
    return 'dev'


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


def create_spark_session(env):
    """Create Spark session with MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    builder = SparkSession.builder \
        .appName(f"membershiptransactionmonthly_ranking_{env}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1")

    # Add MongoDB configuration
    builder = builder \
        .config("spark.mongodb.read.connection.uri", mongo_config['host']) \
        .config("spark.mongodb.write.connection.uri", mongo_config['host']) \
        .config("spark.mongodb.read.database", mongo_config['database']) \
        .config("spark.mongodb.write.database", mongo_config['database'])

    if 'auth_source' in mongo_config:
        builder = builder \
            .config("spark.mongodb.auth.source", mongo_config['auth_source'])

    return builder.getOrCreate()


def get_mongodb_connection(env):
    """Create MongoDB client connection"""
    mongo_config = get_mongodb_config(env)
    try:
        if env == 'dev':
            client = MongoClient(mongo_config['host'])
        else:  # prod
            client = MongoClient(
                mongo_config['host'],
                authSource=mongo_config['auth_source']
            )
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        raise


def get_current_month_year():
    """Get current month and year in Vietnam timezone"""
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)
    return now.month, now.year


def validate_month_year(month, year):
    """Validate if month and year are within acceptable ranges"""
    try:
        if not (1 <= month <= 12):
            raise ValueError(f"Month must be between 1 and 12, got {month}")

        if not (2000 <= year <= 2100):
            raise ValueError(f"Year must be between 2000 and 2100, got {year}")

        # Try to create a date to validate
        datetime(year, month, 1)
        return True
    except ValueError as e:
        print(f"Invalid date: {str(e)}")
        return False


def calculate_unique_ranks(month=None, year=None, env='dev'):
    """Calculate and update unique ranks for the specified month or current month"""
    spark = None
    mongo_client = None

    try:
        # If month/year not provided, use current
        if month is None or year is None:
            month, year = get_current_month_year()

        # Validate the date
        if not validate_month_year(month, year):
            raise ValueError(f"Invalid month/year combination: {month}/{year}")

        print(f"Processing rankings for month: {month}, year: {year} in {env.upper()} environment")

        # Initialize connections
        spark = create_spark_session(env)
        mongo_client = get_mongodb_connection(env)
        mongo_config = get_mongodb_config(env)
        db = mongo_client[mongo_config['database']]

        # Determine collection name
        collection_name = f"{year}_{month}"
        print(f"Using collection: {collection_name}")

        # Read current month's data from MongoDB
        df = spark.read \
            .format("mongodb") \
            .option("collection", collection_name) \
            .option("readConcern.level", "majority") \
            .load()

        count = df.count()
        if count == 0:
            print(f"No data found for month {month}, year {year}")
            return

        print(f"Found {count} records to process")

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
            col("month"),
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
                        "month": row["month"],
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


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process membership transaction monthly rankings')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--env', type=str, choices=['dev', 'prod'], help='Environment (dev or prod)')

    # Handle arguments for both direct running and spark-submit
    if '--month' in sys.argv or '--year' in sys.argv or '--env' in sys.argv:
        args = parser.parse_args()
    else:
        # Handle case when script is run with spark-submit where args come after script
        start_idx = sys.argv.index(__file__) + 1 if __file__ in sys.argv else 1
        args = parser.parse_args(sys.argv[start_idx:])

    return args


def main():
    """Main entry point of the script"""
    print(f"Starting MembershipTransactionMonthly Ranking Process at {datetime.now()}")

    # Determine environment
    env = get_environment()
    print(f"Running in {env.upper()} environment")

    # Parse command line arguments
    args = parse_arguments()

    # Override environment if specified in args
    if args.env:
        env = args.env
        print(f"Environment overridden to: {env.upper()}")

    # Get month and year from arguments or use current
    month = args.month
    year = args.year

    if month is not None and year is not None:
        print(f"Processing data for specified month: {month}/{year}")
    else:
        curr_month, curr_year = get_current_month_year()
        month = month or curr_month
        year = year or curr_year
        print(f"Using month: {month}/{year} (current: {curr_month}/{curr_year})")

    # Run the ranking calculation with environment
    calculate_unique_ranks(month, year, env)

    print(f"MembershipTransactionMonthly Ranking Process Completed at {datetime.now()}")


if __name__ == "__main__":
    main()