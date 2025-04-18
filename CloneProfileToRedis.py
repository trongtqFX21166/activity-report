#!/usr/bin/env python
"""
CloneProfileToRedis.py - Loads profiles from PostgreSQL to Redis
Compatible with Spark cluster mode deployment via Airflow

Usage:
    spark-submit CloneProfileToRedis.py --month [MONTH] --year [YEAR] --env [dev|prod]

Parameters:
    --month      : Month (for logging purposes), defaults to current month
    --year       : Year (for logging purposes), defaults to current year
    --env        : Environment to use (dev or prod), defaults to 'dev'
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time
from datetime import datetime
import logging
import sys
import argparse
import os

# Configure logging directly to stdout for better visibility in YARN logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("ProfileRedisLoader")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Clone profiles from PostgreSQL to Redis')
    parser.add_argument('--month', type=int, help='Month (for logging purposes)', default=datetime.now().month)
    parser.add_argument('--year', type=int, help='Year (for logging purposes)', default=datetime.now().year)
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')

    args = parser.parse_args()

    # If environment not specified in args, check environment variables
    if not args.env:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        if env in ['dev', 'prod']:
            args.env = env

    return args


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


def get_redis_config(env):
    """Return Redis configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-profile-idx',
            'key_prefix': 'activity-dev:profile:'
        }
    else:  # prod
        return {
            'host': '192.168.11.84',
            'port': 6379,
            'password': 'oPn7NjDi569uCriqYbm9iCvR',
            'decode_responses': True,
            'index_name': 'activity-profile-idx',
            'key_prefix': 'activity:profile:'
        }


def create_spark_session(env):
    """Create and return a SparkSession with environment-specific configuration"""
    app_name = f"ProfileRedisLoader-{env}"

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def load_profiles_from_postgres(spark, env):
    """Load profiles from PostgreSQL using direct connection and convert to DataFrame"""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    postgres_config = get_postgres_config(env)
    logger.info(f"Loading profiles from PostgreSQL ({env} environment)")
    logger.info(f"PostgreSQL host: {postgres_config['host']}, database: {postgres_config['database']}")

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=postgres_config["host"],
            database=postgres_config["database"],
            user=postgres_config["user"],
            password=postgres_config["password"]
        )

        # Query profiles - now include MembershipCode field
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                'SELECT "Id", "Phone", "Level", "TotalPoints", "Rank", "LastModified", "MembershipCode" FROM public."Profiles"'
            )
            profiles = cursor.fetchall()

        # Close connection
        conn.close()

        # Convert dictionary rows to a list of Row objects
        from pyspark.sql import Row
        row_data = []
        for profile in profiles:
            # Convert None values to appropriate defaults
            row = {
                "Id": profile["Id"] if profile["Id"] is not None else "",
                "Phone": profile["Phone"] if profile["Phone"] is not None else "",
                "Level": profile["Level"] if profile["Level"] is not None else 1,
                "TotalPoints": profile["TotalPoints"] if profile["TotalPoints"] is not None else 0,
                "Rank": profile["Rank"] if profile["Rank"] is not None else 1,
                "LastModified": profile["LastModified"],
                "MembershipCode": profile["MembershipCode"] if profile["MembershipCode"] is not None else "Level1"
            }
            row_data.append(Row(**row))

        # Create the DataFrame directly
        if row_data:
            profiles_df = spark.createDataFrame(row_data)
        else:
            # Create an empty dataframe with the expected schema
            schema = StructType([
                StructField("Id", StringType(), True),
                StructField("Phone", StringType(), True),
                StructField("Level", IntegerType(), True),
                StructField("TotalPoints", IntegerType(), True),
                StructField("Rank", IntegerType(), True),
                StructField("LastModified", TimestampType(), True),
                StructField("MembershipCode", StringType(), True)
            ])
            profiles_df = spark.createDataFrame([], schema)

        # Log the count and schema
        count = profiles_df.count()
        logger.info(f"Loaded {count} profiles from PostgreSQL")
        logger.info("Schema:")
        profiles_df.printSchema()

        return profiles_df

    except Exception as e:
        logger.error(f"Error loading profiles from PostgreSQL: {str(e)}")
        # Create an empty dataframe with the expected schema
        schema = StructType([
            StructField("Id", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("Level", IntegerType(), True),
            StructField("TotalPoints", IntegerType(), True),
            StructField("Rank", IntegerType(), True),
            StructField("LastModified", TimestampType(), True),
            StructField("MembershipCode", StringType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)
        return empty_df


def get_membership_name(code):
    """Map membership code to a human-readable name"""
    membership_map = {
        "Level1": "Nhập môn",
        "Level2": "Ngôi sao đang lên",
        "Level3": "Uy tín",
        "Level4": "Nổi tiếng",
        "Level5": "Tối thượng"
    }
    return membership_map.get(code, "Nhập môn")


def process_profiles(profiles_df):
    """Process profiles dataframe to prepare for Redis loading"""
    # Convert timestamp columns to Unix timestamps
    profiles_df = profiles_df \
        .withColumn("LastUpdated",
                    when(col("LastModified").isNotNull(), unix_timestamp(col("LastModified")))
                    .otherwise(unix_timestamp(current_timestamp())))

    # Add membership name based on membership code
    profiles_df = profiles_df \
        .withColumn("MembershipName", expr("CASE " +
                                          "WHEN MembershipCode = 'Level1' THEN 'Nhập môn' " +
                                          "WHEN MembershipCode = 'Level2' THEN 'Ngôi sao đang lên' " +
                                          "WHEN MembershipCode = 'Level3' THEN 'Uy tín' " +
                                          "WHEN MembershipCode = 'Level4' THEN 'Nổi tiếng' " +
                                          "WHEN MembershipCode = 'Level5' THEN 'Tối thượng' " +
                                          "ELSE 'Nhập môn' END"))

    # Select only the fields we need (matching Redis schema)
    # Make sure we handle any missing columns
    columns_to_select = []

    if "Phone" in profiles_df.columns:
        columns_to_select.append(col("Phone"))
    else:
        columns_to_select.append(lit("unknown").alias("Phone"))

    if "Level" in profiles_df.columns:
        columns_to_select.append(col("Level").cast(IntegerType()))
    else:
        columns_to_select.append(lit(1).alias("Level"))

    if "TotalPoints" in profiles_df.columns:
        columns_to_select.append(col("TotalPoints").cast(IntegerType()))
    else:
        columns_to_select.append(lit(0).alias("TotalPoints"))

    if "Rank" in profiles_df.columns:
        columns_to_select.append(col("Rank").cast(IntegerType()))
    else:
        columns_to_select.append(lit(0).alias("Rank"))

    if "LastUpdated" in profiles_df.columns:
        columns_to_select.append(col("LastUpdated").cast(LongType()))
    else:
        columns_to_select.append(unix_timestamp(current_timestamp()).alias("LastUpdated"))

    # Add the membership fields
    if "MembershipCode" in profiles_df.columns:
        columns_to_select.append(col("MembershipCode"))
    else:
        columns_to_select.append(lit("Level1").alias("MembershipCode"))

    if "MembershipName" in profiles_df.columns:
        columns_to_select.append(col("MembershipName"))
    else:
        columns_to_select.append(lit("Nhập môn").alias("MembershipName"))

    processed_df = profiles_df.select(*columns_to_select)

    # Handle null values
    processed_df = processed_df \
        .fillna({
            "Phone": "unknown",
            "Level": 1,
            "TotalPoints": 0,
            "Rank": 999999,
            "LastUpdated": int(time.time()),
            "MembershipCode": "Level1",
            "MembershipName": "Nhập môn"
        })

    # Add a filter to ensure Phone is not null or empty
    processed_df = processed_df.filter(col("Phone").isNotNull() & (col("Phone") != ""))

    return processed_df


def save_to_redis(df, env, batch_size=500):
    """Save dataframe to Redis - running on driver for simplicity"""
    from redis import Redis
    import json

    # Get Redis configuration for the environment
    redis_config = get_redis_config(env)

    # Log the function execution
    logger.info(f"Starting save_to_redis with {df.count()} records to Redis ({env} environment)")
    logger.info(f"Redis host: {redis_config['host']}, port: {redis_config['port']}")

    # First collect data to driver
    rows = df.collect()
    logger.info(f"Collected {len(rows)} rows to driver")

    # Establish Redis connection on driver
    redis_client = None
    try:
        # Create Redis connection
        redis_client = Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            password=redis_config['password'],
            decode_responses=redis_config['decode_responses']
        )

        # Test connection
        redis_client.ping()
        logger.info("Successfully connected to Redis")

        # Process in batches
        total_rows = len(rows)
        total_success = 0
        total_errors = 0

        # Split into batches
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]

            # Process batch
            pipeline = redis_client.pipeline(transaction=False)
            success_count = 0
            error_count = 0

            for row in batch:
                try:
                    # Generate Redis key
                    redis_key = f"{redis_config['key_prefix']}{row['Phone']}"

                    # Create Redis document - matching schema exactly
                    profile_doc = {
                        "Phone": row["Phone"],
                        "Level": int(row["Level"]) if row["Level"] is not None else 1,
                        "TotalPoints": int(row["TotalPoints"]) if row["TotalPoints"] is not None else 0,
                        "Rank": int(row["Rank"]) if row["Rank"] is not None else 0,
                        "LastUpdated": int(row["LastUpdated"]) if row["LastUpdated"] is not None else int(time.time()),
                        "MembershipCode": row["MembershipCode"] if row["MembershipCode"] is not None else "Level1",
                        "MembershipName": row["MembershipName"] if row["MembershipName"] is not None else "Nhập môn"
                    }

                    # Add to pipeline
                    pipeline.json().set(redis_key, "$", profile_doc)
                    pipeline.sadd(redis_config['index_name'], redis_key)
                    success_count += 1

                except Exception as e:
                    logger.error(f"Error preparing Redis document for {row['Phone']}: {str(e)}")
                    error_count += 1

            # Execute pipeline
            try:
                pipeline.execute()
                logger.info(f"Batch {i // batch_size + 1}: Successfully loaded {success_count} profiles")
                total_success += success_count
            except Exception as e:
                logger.error(f"Error executing Redis pipeline: {str(e)}")
                # Count all as failures
                total_errors += success_count + error_count

            # Sleep briefly between batches
            time.sleep(0.1)

        # Return stats
        return total_rows, total_success, total_errors

    except Exception as e:
        logger.error(f"Error in save_to_redis: {str(e)}")
        return 0, 0, 0
    finally:
        if redis_client:
            redis_client.close()


def main():
    """Main entry point for the Spark application"""
    # Parse arguments to get month, year, and environment
    args = parse_arguments()
    month = args.month
    year = args.year
    env = args.env

    spark = None

    try:
        # Initialize Spark with environment-specific configuration
        spark = create_spark_session(env)

        # Log app info
        logger.info(f"Starting Profile to Redis Batch Process for {month}/{year} in {env} environment")

        # Load profiles from PostgreSQL
        profiles_df = load_profiles_from_postgres(spark, env)

        if profiles_df.count() == 0:
            logger.warning("No profiles found in database")
            return

        # Process profiles
        processed_df = process_profiles(profiles_df)

        # Show sample data
        logger.info("Sample processed data:")
        processed_df.show(5, truncate=False)

        # Save to Redis with environment-specific configuration
        total_rows, success_count, error_count = save_to_redis(processed_df, env, 1000)

        # Log summary
        logger.info(f"Profile to Redis Batch Process completed")
        logger.info(f"Summary: Processed {total_rows} profiles, Success: {success_count}, Failed: {error_count}")

    except Exception as e:
        logger.error(f"Profile to Redis Batch Process failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()