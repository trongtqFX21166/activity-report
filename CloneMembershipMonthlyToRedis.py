from pyspark.sql import SparkSession
# Import specific functions instead of using wildcard to avoid shadowing built-ins
from pyspark.sql.functions import col, desc, asc, lit, current_timestamp
from redis import Redis
import json
import sys
import argparse
from datetime import datetime
import logging
import os
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CloneMembershipMonthlyToRedis")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Clone membership data from MongoDB to Redis for a specific month')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2024)')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')
    parser.add_argument('-m', type=int, help='Month to process (1-12)')
    parser.add_argument('-y', type=int, help='Year to process (e.g., 2024)')

    # Parse known args to handle cases where additional arguments exist
    args, _ = parser.parse_known_args()

    # Allow both --month and -m formats (same for year)
    month = args.month or args.m
    year = args.year or args.y

    # If environment not specified in args, check environment variables
    if not args.env:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        if env in ['dev', 'prod']:
            args.env = env

    # Validate month if provided
    if month is not None and not (1 <= month <= 12):
        logger.error(f"Error: Month must be between 1 and 12, got {month}")
        sys.exit(1)

    return month, year, args.env


def get_mongodb_config(env):
    """Return MongoDB configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': 'mongodb://192.168.10.97:27017',
            'database': 'activity_membershiptransactionmonthly_dev'
        }
    else:  # prod
        # Use the full connection string with authentication details
        # Use admin database for authentication source (default)
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017/?authSource=admin',
            'database': 'activity_membershiptransactionmonthly',
            'auth_source': 'admin'
        }


def get_mongodb_client(env):
    """Create and return MongoDB client based on environment"""
    mongo_config = get_mongodb_config(env)
    try:
        # Import here to ensure availability
        from pymongo import MongoClient

        # Create MongoDB client with appropriate settings
        if env == 'dev':
            # Development environment - no auth
            client = MongoClient(mongo_config['host'])
        else:  # prod
            # Production environment - parse connection string which includes auth credentials
            # The connection string format: mongodb://username:password@host1,host2,host3/database?authSource=admin
            client = MongoClient(
                mongo_config['host'],
                authSource=mongo_config.get('auth_source', 'admin')
            )

        # Test connection with proper error handling
        try:
            # Ping the database to verify connection works
            client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB ({env} environment)")
        except Exception as ping_error:
            logger.error(f"Failed to ping MongoDB: {str(ping_error)}")
            # Try a different approach to test connection
            try:
                # Just list databases to check connectivity
                client.list_database_names()
                logger.info(f"Successfully listed databases from MongoDB ({env} environment)")
            except Exception as list_error:
                logger.error(f"Failed to list databases: {str(list_error)}")
                raise

        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise


def get_redis_config(env):
    """Return Redis configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-ranking-monthly-idx',
            'key_prefix': 'activity-dev:membership:monthly:'
        }
    else:  # prod
        return {
            'host': '192.168.11.84',
            'port': 6379,
            'password': 'oPn7NjDi569uCriqYbm9iCvR',
            'decode_responses': True,
            'index_name': 'activity-ranking-monthly-idx',
            'key_prefix': 'activity:membership:monthly:'
        }


def create_spark_session(env):
    """Create Spark session with MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    # Extract the host without the mongodb:// prefix for clarity in logs
    host_for_logging = mongo_config['host'].replace('mongodb://', '').split('/?')[0]
    logger.info(f"Configuring Spark to connect to MongoDB at {host_for_logging}")

    # Build the SparkSession with the MongoDB connector package
    builder = SparkSession.builder \
        .appName(f"membership-monthly-redis-sync-{env}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1")

    # Create and return the SparkSession
    spark = builder.getOrCreate()

    # Log the created session
    logger.info(f"Created Spark session with app ID: {spark.sparkContext.applicationId}")

    return spark


def setup_redis_connection(env):
    """Setup Redis connection with environment-specific configuration"""
    redis_config = get_redis_config(env)

    try:
        redis_client = Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            password=redis_config['password'],
            decode_responses=redis_config['decode_responses']
        )
        redis_client.ping()
        logger.info(f"Successfully connected to Redis ({env} environment)")
        return redis_client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        sys.exit(1)


def generate_document_id(phone, month, year, env):
    """Generate Redis document ID following the index pattern"""
    redis_config = get_redis_config(env)
    return f"{redis_config['key_prefix']}{year}:{month}:{phone}"


def process_batch(batch_df, redis_client, env):
    """Process a batch of records and upsert to Redis"""
    try:
        records = batch_df.collect()
        logger.info(f"Processing {len(records)} records in batch")

        inserts = 0
        updates = 0
        errors = 0

        # Get Redis configuration
        redis_config = get_redis_config(env)
        index_name = redis_config['index_name']

        # Process in smaller sub-batches for better control
        sub_batch_size = 50
        for i in range(0, len(records), sub_batch_size):
            sub_batch = records[i:i + sub_batch_size]
            pipeline = redis_client.pipeline(transaction=False)  # Use non-transactional pipeline for better performance

            sub_batch_inserts = 0
            for record in sub_batch:
                try:
                    # Generate document ID
                    doc_id = generate_document_id(record['phone'], record['month'], record['year'], env)
                    logger.debug(f"Processing: {doc_id}")

                    # Create new document
                    new_doc = {
                        "Id": doc_id,
                        "Phone": record['phone'],
                        "MembershipCode": record['membershipcode'],
                        "Month": record['month'],
                        "Year": record['year'],
                        "Rank": record['rank'],
                        "MembershipName": record['membershipname'],
                        "TotalPoints": record['totalpoints']
                    }

                    # Queue delete and set operations
                    pipeline.delete(doc_id)
                    pipeline.json().set(doc_id, "$", new_doc)
                    pipeline.sadd(index_name, doc_id)
                    sub_batch_inserts += 1

                except Exception as e:
                    logger.error(f"Error preparing record {record['phone']}: {str(e)}")
                    errors += 1
                    continue

            # Execute pipeline for sub-batch with retry mechanism
            max_retries = 3
            retry_count = 0
            retry_delay = 1  # seconds

            while retry_count < max_retries:
                try:
                    pipeline.execute()
                    inserts += sub_batch_inserts
                    break  # Success, exit retry loop
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Error executing pipeline (attempt {retry_count}/{max_retries}): {str(e)}")

                    if retry_count >= max_retries:
                        logger.error(f"Max retries reached. Processing records individually.")
                        # If pipeline fails after all retries, try records individually
                        for record in sub_batch:
                            try:
                                doc_id = generate_document_id(record['phone'], record['month'], record['year'], env)
                                new_doc = {
                                    "Id": doc_id,
                                    "Phone": record['phone'],
                                    "MembershipCode": record['membershipcode'],
                                    "Month": record['month'],
                                    "Year": record['year'],
                                    "Rank": record['rank'],
                                    "MembershipName": record['membershipname'],
                                    "TotalPoints": record['totalpoints']
                                }

                                # Clean up and retry individual record
                                redis_client.delete(doc_id)
                                redis_client.json().set(doc_id, "$", new_doc)
                                redis_client.sadd(index_name, doc_id)
                                inserts += 1
                            except Exception as individual_error:
                                logger.error(
                                    f"Error processing individual record {record['phone']}: {str(individual_error)}")
                                errors += 1
                    else:
                        # Wait before retrying
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff

        logger.info(f"Sub-batch processing details:")
        logger.info(f"  Records to process: {len(records)}")
        logger.info(f"  Successfully inserted: {inserts}")
        logger.info(f"  Errors: {errors}")

        return inserts, updates, errors

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        raise


def read_from_mongodb(spark, month, year, env):
    """Read data from MongoDB with specified month/year filter"""
    collection_name = f"{year}_{month}"
    mongo_config = get_mongodb_config(env)
    database = mongo_config['database']
    connection_string = mongo_config['host']

    logger.info(f"Reading from MongoDB: {database}.{collection_name}")
    logger.info(f"Using connection string: {connection_string}")

    # For MongoDB connector v4+, we need to use the full connection string with explicit database
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", connection_string) \
        .option("database", database) \
        .option("collection", collection_name) \
        .load()

    return df


def process_data(spark, redis_client, month=None, year=None, env='dev'):
    """Main data processing function"""
    try:
        # Use provided month and year or default to current month/year
        if month is None or year is None:
            current_date = datetime.now()
            month = month or current_date.month
            year = year or current_date.year

        logger.info(f"Starting data processing for {month}/{year} in {env} environment...")

        # Read from MongoDB with specified month/year filter - passing the env parameter
        df = read_from_mongodb(spark, month, year, env)
        total_records = df.count()
        logger.info(f"Found {total_records} records to process")

        if total_records == 0:
            logger.info(f"No records found for {month}/{year}")
            return

        # Configure batching
        batch_size = 200  # Smaller batch size for better control

        # Repartition the dataframe for more efficient processing
        # Safely calculate number of partitions using built-in max directly
        num_partitions = 1
        if total_records > 0:
            num_partitions = max(1, (total_records + batch_size - 1) // batch_size)

        # If we're on a 'prod' environment, limit the max partitions to prevent overloading
        if env == 'prod':
            num_partitions = min(num_partitions, 20)  # Cap at 20 partitions for production

        logger.info(f"Using {num_partitions} partitions for processing {total_records} records")
        df_partitioned = df.repartition(num_partitions)

        total_inserts = 0
        total_updates = 0
        total_errors = 0

        # Process each partition
        partitions = df_partitioned.rdd.mapPartitions(lambda it: [list(it)]).collect()

        for batch_num, partition in enumerate(partitions, 1):
            logger.info(f"\nProcessing batch {batch_num}/{len(partitions)}")
            logger.info(f"Batch size: {len(partition)} records")

            # Create a new DataFrame for the partition
            batch_df = spark.createDataFrame(partition, df.schema)

            # Convert column names to lowercase to match Redis document structure
            for column in batch_df.columns:
                batch_df = batch_df.withColumnRenamed(column, column.lower())

            inserts, updates, errors = process_batch(batch_df, redis_client, env)
            total_inserts += inserts
            total_updates += updates
            total_errors += errors

            logger.info(f"Completed batch {batch_num}/{len(partitions)}")

        logger.info(f"\nProcessing Summary:")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Total inserts: {total_inserts}")
        logger.info(f"Total updates: {total_updates}")
        logger.info(f"Total errors: {total_errors}")

    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")
        raise


def main():
    """Main entry point of the script"""
    start_time = datetime.now()

    try:
        # Parse command line arguments
        month, year, env = parse_arguments()

        if month and year:
            logger.info(
                f"Starting Membership Monthly Redis Sync Job at {start_time} for {month}/{year} in {env} environment")
        else:
            logger.info(
                f"Starting Membership Monthly Redis Sync Job at {start_time} for current month in {env} environment")

        spark = None
        redis_client = None
        mongo_client = None

        # Verify MongoDB connection first to fail fast if there's an auth problem
        try:
            logger.info(f"Testing MongoDB connection for {env} environment...")
            mongo_client = get_mongodb_client(env)
            # Test successful, close the connection for now
            mongo_client.close()
            mongo_client = None
            logger.info("MongoDB connection test successful")
        except Exception as mongo_error:
            logger.error(f"MongoDB connection test failed: {str(mongo_error)}")
            raise

        # Initialize Spark
        spark = create_spark_session(env)

        # Initialize Redis
        redis_client = setup_redis_connection(env)

        # Process data for specified month/year or current month/year
        process_data(spark, redis_client, month, year, env)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Job completed successfully at {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)

    finally:
        if mongo_client:
            mongo_client.close()
        if spark:
            spark.stop()
        if redis_client:
            redis_client.close()


if __name__ == "__main__":
    main()