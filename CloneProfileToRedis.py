#!/usr/bin/env python
"""
CloneProfileToRedis.py - Loads profiles from PostgreSQL to Redis
Compatible with Spark cluster mode deployment via Airflow
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ProfileRedisLoader")

# Configuration
POSTGRES_CONFIG = {
    "host": "192.168.8.230",
    "database": "TrongTestDB1",
    "user": "postgres",
    "password": "admin123."
}

REDIS_CONFIG = {
    'host': '192.168.8.226',
    'port': 6379,
    'password': '0ef1sJm19w3OKHiH',
    'decode_responses': True
}

# Redis configuration
REDIS_PREFIX = "activity-dev:profile:"  # Prefix for profile data keys
REDIS_INDEX = "activity-profile-idx"  # Index name, must match initProfileCache.py


def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("ProfileRedisLoader") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def load_profiles_from_postgres(spark):
    """Load profiles from PostgreSQL using direct connection and convert to DataFrame"""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["host"],
            database=POSTGRES_CONFIG["database"],
            user=POSTGRES_CONFIG["user"],
            password=POSTGRES_CONFIG["password"]
        )

        # Query profiles
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                'SELECT "Id", "Phone", "Level", "TotalPoints", "Rank", "LastModified" FROM public."Profiles"'
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
                "LastModified": profile["LastModified"]
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
                StructField("LastModified", TimestampType(), True)
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
            StructField("LastModified", TimestampType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)
        return empty_df


def process_profiles(profiles_df):
    """Process profiles dataframe to prepare for Redis loading"""
    # Convert timestamp columns to Unix timestamps
    profiles_df = profiles_df \
        .withColumn("LastUpdated",
                    when(col("LastModified").isNotNull(), unix_timestamp(col("LastModified")))
                    .otherwise(unix_timestamp(current_timestamp())))

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

    processed_df = profiles_df.select(*columns_to_select)

    # Handle null values
    processed_df = processed_df \
        .fillna({"Phone": "unknown", "Level": 1, "TotalPoints": 0, "Rank": 999999, "LastUpdated": int(time.time())})

    # Add a filter to ensure Phone is not null or empty
    processed_df = processed_df.filter(col("Phone").isNotNull() & (col("Phone") != ""))

    return processed_df


def save_to_redis(df, batch_size=50):
    """Save dataframe to Redis - running on driver for simplicity

    Given the error we've seen with distributed execution, this function
    now runs on the driver to avoid potential issues with Redis libraries
    or network connectivity from worker nodes.
    """
    from redis import Redis
    import json

    # Log the function execution
    logger.info(f"Starting save_to_redis with {df.count()} records")

    # First collect data to driver
    rows = df.collect()
    logger.info(f"Collected {len(rows)} rows to driver")

    # Establish Redis connection on driver
    redis_client = None
    try:
        # Create Redis connection
        redis_client = Redis(
            host=REDIS_CONFIG['host'],
            port=REDIS_CONFIG['port'],
            password=REDIS_CONFIG['password'],
            decode_responses=REDIS_CONFIG['decode_responses']
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
                    redis_key = f"{REDIS_PREFIX}{row['Phone']}"

                    # Create Redis document - matching schema exactly
                    profile_doc = {
                        "Phone": row["Phone"],
                        "Level": int(row["Level"]) if row["Level"] is not None else 1,
                        "TotalPoints": int(row["TotalPoints"]) if row["TotalPoints"] is not None else 0,
                        "Rank": int(row["Rank"]) if row["Rank"] is not None else 0,
                        "LastUpdated": int(row["LastUpdated"]) if row["LastUpdated"] is not None else int(time.time())
                    }

                    # Add to pipeline
                    pipeline.delete(redis_key)
                    pipeline.json().set(redis_key, "$", profile_doc)
                    pipeline.sadd(REDIS_INDEX, redis_key)
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
    parser = argparse.ArgumentParser(description='Clone profiles from PostgreSQL to Redis')
    parser.add_argument('--month', type=int, help='Month (for logging purposes)', default=datetime.now().month)
    parser.add_argument('--year', type=int, help='Year (for logging purposes)', default=datetime.now().year)
    args = parser.parse_args()

    spark = None

    try:
        # Initialize Spark
        spark = create_spark_session()

        # Log app info
        logger.info(f"Starting Profile to Redis Batch Process for {args.month}/{args.year}")

        # Load profiles from PostgreSQL
        profiles_df = load_profiles_from_postgres(spark)

        if profiles_df.count() == 0:
            logger.warning("No profiles found in database")
            return

        # Process profiles
        processed_df = process_profiles(profiles_df)

        # Show sample data
        logger.info("Sample processed data:")
        processed_df.show(5, truncate=False)

        # Save to Redis
        total_rows, success_count, error_count = save_to_redis(processed_df)

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