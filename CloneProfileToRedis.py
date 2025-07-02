#!/usr/bin/env python
"""
CloneProfileToRedis.py - Loads profiles from PostgreSQL to Redis
Compatible with Spark cluster mode deployment via Airflow
With support for both Redis standalone and Redis Cluster configurations

Usage:
    spark-submit CloneProfileToRedis.py --month [MONTH] --year [YEAR] --env [dev|prod]

Parameters:
    --month      : Month (for logging purposes), defaults to current month
    --year       : Year (for logging purposes), defaults to current year
    --env        : Environment to use (dev or prod), defaults to 'dev'
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, expr, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
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
    # Production environment
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
            'is_cluster': False,
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-profile-idx',
            'key_prefix': 'activity_profile:'
        }
    # Production environment uses Redis Cluster
    return {
        'is_cluster': True,
        'nodes': [
            {'host': '192.168.11.227', 'port': 6379},
            {'host': '192.168.11.228', 'port': 6379},
            {'host': '192.168.11.229', 'port': 6379}
        ],
        'password': '',
        'decode_responses': True,
        'index_name': 'activity-profile-idx',
        'key_prefix': 'activity_profile:'
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

        # Create an empty dataframe if no data found
        if not row_data:
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


def setup_redis_connection(env):
    """Setup Redis connection with environment-specific configuration"""
    from redis import Redis
    from redis.cluster import RedisCluster, ClusterNode

    redis_config = get_redis_config(env)

    try:
        # Check if we should use cluster mode
        if redis_config.get('is_cluster', False):
            # Convert node dictionaries to ClusterNode objects
            cluster_nodes = [
                ClusterNode(host=node['host'], port=node['port'])
                for node in redis_config['nodes']
            ]

            logger.info(f"Connecting to Redis Cluster in {env} environment...")
            logger.info(f"Cluster nodes: {', '.join([f'{node.host}:{node.port}' for node in cluster_nodes])}")

            # Create Redis Cluster client
            redis_client = RedisCluster(
                startup_nodes=cluster_nodes,
                password=redis_config['password'],
                decode_responses=redis_config['decode_responses'],
                skip_full_coverage_check=True  # More resilient to node failures
            )
            # Test connection
            redis_client.ping()
            logger.info(f"Successfully connected to Redis Cluster")
            return redis_client

        # Standalone Redis connection (if not cluster mode)
        logger.info(
            f"Connecting to standalone Redis in {env} environment at {redis_config['host']}:{redis_config['port']}")
        redis_client = Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            password=redis_config['password'],
            decode_responses=redis_config['decode_responses']
        )
        # Test connection
        redis_client.ping()
        logger.info(f"Successfully connected to standalone Redis")
        return redis_client

    except Exception as e:
        logger.error(
            f"Failed to connect to Redis{' Cluster' if redis_config.get('is_cluster', False) else ''}: {str(e)}")
        sys.exit(1)


def check_index_exists(redis_client, index_name, env):
    """Check if the search index exists, and create it if it doesn't"""
    try:
        # Check if index exists
        redis_client.ft(index_name).info()
        logger.info(f"Search index {index_name} exists")
        return True
    except Exception as e:
        if "unknown index name" in str(e).lower():
            logger.warning(f"Search index {index_name} doesn't exist. Creating it now...")
            create_profile_index(redis_client, index_name, get_redis_config(env)['key_prefix'])
            return True
        # Other errors
        logger.error(f"Error checking index: {str(e)}")
        return False


def create_profile_index(redis_client, index_name, prefix):
    """Create RediSearch index for Profile Cache"""
    try:
        from redis.commands.search.field import TextField, NumericField, TagField
        from redis.commands.search.indexDefinition import IndexDefinition, IndexType

        try:
            # Try to drop existing index
            redis_client.ft(index_name).dropindex(delete_documents=False)
            logger.info(f"Dropped existing index: {index_name}")
        except Exception as e:
            logger.warning(f"No existing index to drop or error dropping index: {str(e)}")

        # Define schema as per requirements
        schema = (
            # Phone field made both searchable and sortable for flexible querying
            TextField("$.Phone", as_name="Phone", sortable=True),

            # Profile level and points
            NumericField("$.Level", as_name="Level", sortable=True),
            NumericField("$.TotalPoints", as_name="TotalPoints", sortable=True),
            NumericField("$.Rank", as_name="Rank", sortable=True),

            TextField("$.MembershipCode", as_name="MembershipCode", sortable=True),
            TextField("$.MembershipName", as_name="MembershipName", sortable=True),

            # Optional additional fields that might be useful
            NumericField("$.LastUpdated", as_name="LastUpdated", sortable=True)
        )

        # Create the index
        redis_client.ft(index_name).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[prefix],
                index_type=IndexType.JSON
            )
        )

        logger.info(f"""
RediSearch index created successfully:
- Index Name: {index_name}
- Prefix: {prefix}
- Schema:
  - Phone (Text, Sortable, Searchable)
  - Level (Numeric, Sortable)
  - TotalPoints (Numeric, Sortable)
  - Rank (Numeric, Sortable)
  - MembershipCode (Text, Sortable)
  - MembershipName (Text, Sortable)
  - LastUpdated (Numeric, Sortable)
""")

        # Test the index
        info = redis_client.ft(index_name).info()
        logger.info("\nIndex Info:")
        for key, value in info.items():
            logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Failed to create index: {str(e)}")
        raise


def save_to_redis(df, env, batch_size=200):
    """Save dataframe to Redis - handles both standalone and cluster modes"""
    # Get Redis configuration for the environment
    redis_config = get_redis_config(env)

    # Log the function execution
    logger.info(f"Starting save_to_redis with {df.count()} records to Redis ({env} environment)")

    if redis_config.get('is_cluster', False):
        logger.info(
            f"Using Redis Cluster mode with nodes")
    else:
        logger.info(f"Using standalone Redis at {redis_config['host']}:{redis_config['port']}")

    # First collect data to driver
    rows = df.collect()
    logger.info(f"Collected {len(rows)} rows to driver")

    # Establish Redis connection on driver
    redis_client = None

    try:
        # Create Redis connection
        redis_client = setup_redis_connection(env)

        # Check if the index exists, create if needed
        index_name = redis_config['index_name']
        check_index_exists(redis_client, index_name, env)

        # Process in batches
        total_rows = len(rows)
        total_success = 0
        total_errors = 0

        # Split into batches
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            batch_size_actual = len(batch)

            # Calculate total batches
            total_batches = (total_rows + batch_size - 1) // batch_size
            current_batch = i // batch_size + 1

            logger.info(f"Processing batch {current_batch} of {total_batches} ({batch_size_actual} profiles)")

            # Process each profile in the batch
            success_count = 0
            error_count = 0

            # In cluster mode, we need to process individually without pipeline
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

                    # Set in Redis and add to index
                    redis_client.json().set(redis_key, "$", profile_doc)
                    redis_client.sadd(redis_config['index_name'], redis_key)
                    success_count += 1

                except Exception as e:
                    logger.error(f"Error updating Redis for {row['Phone']}: {str(e)}")
                    error_count += 1

            # Log batch progress
            logger.info(f"Batch {current_batch}/{total_batches}: Success: {success_count}, Errors: {error_count}")
            total_success += success_count
            total_errors += error_count

            # Add a small delay between batches to prevent overwhelming Redis
            time.sleep(0.1)

        # Return stats
        logger.info(f"Redis update completed: {total_success} successful, {total_errors} errors")
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
        total_rows, success_count, error_count = save_to_redis(processed_df, env, 500)

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