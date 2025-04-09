from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pymongo import MongoClient, UpdateOne
import pytz
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
            'database': 'activity_membershiptransactionyearly_dev'
        }
    else:  # prod
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017',
            'database': 'activity_membershiptransactionyearly',
            'auth_source': 'admin'
        }


def create_spark_session(env):
    """Create Spark session with MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    builder = SparkSession.builder \
        .appName(f"membershiptransactionyearly_ranking_{env}") \
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


def calculate_unique_ranks(env='dev'):
    """Calculate and update unique ranks for the current month"""
    spark = None
    mongo_client = None

    try:
        # Initialize connections
        spark = create_spark_session(env)
        mongo_client = get_mongodb_connection(env)
        mongo_config = get_mongodb_config(env)
        db = mongo_client[mongo_config['database']]

        # Get current month/year
        current_month, current_year = get_current_month_year()
        collection_name = f"{current_year}"
        print(f"Processing rankings for collection: {collection_name} in {env.upper()} environment")

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


def parse_arguments():
    """Parse command line arguments"""
    import argparse

    parser = argparse.ArgumentParser(description='Process membership transaction yearly rankings')
    parser.add_argument('--env', type=str, choices=['dev', 'prod'], help='Environment (dev or prod)')
    parser.add_argument('--year', type=int, help='Specific year to process (optional)')

    # Handle arguments for both direct running and spark-submit
    if '--env' in sys.argv or '--year' in sys.argv:
        args = parser.parse_args()
    else:
        # Handle case when script is run with spark-submit where args come after script
        start_idx = sys.argv.index(__file__) + 1 if __file__ in sys.argv else 1
        args = parser.parse_args(sys.argv[start_idx:])

    return args


def main():
    """Main entry point of the script"""
    print(f"Starting MembershipTransactionYearly Ranking Process at {datetime.now()}")

    # Determine environment
    env = get_environment()
    print(f"Running in {env.upper()} environment")

    # Parse command line arguments
    args = parse_arguments()

    # Override environment if specified in args
    if args.env:
        env = args.env
        print(f"Environment overridden to: {env.upper()}")

    # Process with environment context
    calculate_unique_ranks(env)

    print(f"MembershipTransactionYearly Ranking Process Completed at {datetime.now()}")


if __name__ == "__main__":
    main()