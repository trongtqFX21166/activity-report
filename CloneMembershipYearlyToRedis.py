from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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
logger = logging.getLogger("membership-yearly-redis-sync")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Clone yearly membership data from MongoDB to Redis')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2024)')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment (dev or prod)')
    parser.add_argument('-y', type=int, help='Year to process (e.g., 2024)')

    # Parse known args to handle cases where additional arguments exist
    args, _ = parser.parse_known_args()

    # Allow both --year and -y formats
    year = args.year or args.y

    # If environment not specified in args, check environment variables
    if not args.env:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        if env in ['dev', 'prod']:
            args.env = env

    return year, args.env


def get_mongodb_config(env):
    """Return MongoDB configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': 'mongodb://192.168.10.97:27017',
            'database': 'activity_membershiptransactionyearly_dev'
        }
    else:  # prod
        # For production, we need to specify the authentication database is 'admin', not the target database
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017/?authSource=admin',
            'database': 'activity_membershiptransactionyearly',
            'auth_source': 'admin'
        }


def get_redis_config(env):
    """Return Redis configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-ranking-yearly-idx',
            'key_prefix': 'activity-dev:membership:yearly:'
        }
    else:  # prod
        return {
            'host': '192.168.11.84',
            'port': 6379,
            'password': 'oPn7NjDi569uCriqYbm9iCvR',
            'decode_responses': True,
            'index_name': 'activity-ranking-yearly-idx',
            'key_prefix': 'activity:membership:yearly:'
        }


def create_spark_session(env):
    """Create Spark session with MongoDB configurations"""
    # Include all necessary MongoDB connector dependencies
    packages = [
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
        "org.mongodb:mongodb-driver-sync:4.8.2",
        "org.mongodb:bson:4.8.2",
        "org.mongodb:mongodb-driver-core:4.8.2"
    ]

    # Create a basic SparkSession builder with MongoDB connector
    builder = SparkSession.builder \
        .appName(f"membership-yearly-redis-sync-{env}") \
        .config("spark.jars.packages", ",".join(packages))

    # Configure MongoDB connection based on environment
    # The key fix: use connection.uri which is the correct property name
    if env == 'prod':
        # Production environment
        mongodb_hosts = "192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017"
        mongodb_user = "admin"
        mongodb_password = "gctStAiH22368l5qziUV"

        # Use connection string format for MongoDB, and the correct property name
        builder = builder \
            .config("spark.mongodb.connection.uri",
                    f"mongodb://{mongodb_user}:{mongodb_password}@{mongodb_hosts}/admin?authSource=admin")

        logger.info(f"Using production MongoDB hosts: {mongodb_hosts}")
    else:
        # Development environment
        mongodb_host = "192.168.10.97:27017"

        # For development, no authentication needed
        builder = builder \
            .config("spark.mongodb.connection.uri", f"mongodb://{mongodb_host}")

        logger.info(f"Using development MongoDB host: {mongodb_host}")

    # Create the SparkSession
    spark = builder.getOrCreate()
    logger.info(f"Created SparkSession with app ID: {spark.sparkContext.applicationId}")

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


def generate_document_id(year, phone, env):
    """Generate Redis document ID following the index pattern for the environment"""
    redis_config = get_redis_config(env)
    return f"{redis_config['key_prefix']}{year}:{phone}"


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
                    doc_id = generate_document_id(record['year'], record['phone'], env)
                    logger.debug(f"Processing: {doc_id}")

                    # Create new document
                    new_doc = {
                        "Id": doc_id,
                        "Phone": record['phone'],
                        "MembershipCode": record['membershipcode'],
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
                                doc_id = generate_document_id(record['year'], record['phone'], env)
                                new_doc = {
                                    "Id": doc_id,
                                    "Phone": record['phone'],
                                    "MembershipCode": record['membershipcode'],
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


def read_from_mongodb(spark, current_year, env):
    """Read data from MongoDB with current year filter"""
    collection_name = f"{current_year}"

    # Get the appropriate database name based on environment
    db_name = "activity_membershiptransactionyearly"
    if env != 'prod':
        db_name += "_dev"

    logger.info(f"Reading data from MongoDB: database={db_name}, collection={collection_name}")

    try:
        # This is the correct format for MongoDB Spark connector 3.x
        df = spark.read \
            .format("mongodb") \
            .option("database", db_name) \
            .option("collection", collection_name) \
            .load()

        # Debug to ensure we're actually connecting to the right place
        logger.info(f"MongoDB connection successful - reading from {db_name}.{collection_name}")

        # Log successful data load
        record_count = df.count()
        logger.info(f"Successfully loaded {record_count} records from MongoDB {db_name}.{collection_name}")
        return df

    except Exception as e:
        logger.error(f"Error reading from MongoDB: {str(e)}")

        # Verbose error information
        logger.error(f"MongoDB connection details:")
        logger.error(f"  Database: {db_name}")
        logger.error(f"  Collection: {collection_name}")

        # Get the current MongoDB connection URI
        try:
            mongo_uri = spark.conf.get("spark.mongodb.connection.uri")
            logger.error(f"  Connection URI being used: {mongo_uri}")
        except:
            logger.error("  Could not retrieve connection URI from Spark configuration")

        raise


def process_data(spark, redis_client, year=None, env='dev'):
    """Main data processing function"""
    try:
        # Use provided year or default to current year
        if year is None:
            current_date = datetime.now()
            year = current_date.year

        logger.info(f"Starting data processing for year {year} in {env} environment...")

        # Read from MongoDB with current year filter, passing environment
        df = read_from_mongodb(spark, year, env)
        total_records = df.count()

        # Log schema and sample data
        logger.info(f"Collection schema:")
        df.printSchema()

        # Show sample data if available
        if total_records > 0:
            logger.info("Sample data (first 5 records):")
            df.show(5, truncate=False)

        logger.info(f"Found {total_records} records to process")

        if total_records == 0:
            logger.info(f"No records found for year {year}")
            return

        # Configure batching
        batch_size = 200  # Smaller batch size for better control

        # Repartition the dataframe for more efficient processing
        # Fix: use built-in max function properly
        if total_records > 0:
            num_partitions = 1 if total_records <= batch_size else (total_records + batch_size - 1) // batch_size
        else:
            num_partitions = 1

        logger.info(f"Processing with {num_partitions} partitions (batch size: {batch_size})")

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
        year, env = parse_arguments()

        if year:
            logger.info(
                f"Starting Membership Yearly Redis Sync Job at {start_time} for year {year} in {env} environment")
        else:
            logger.info(
                f"Starting Membership Yearly Redis Sync Job at {start_time} for current year in {env} environment")

        spark = None
        redis_client = None

        # Get MongoDB configuration
        mongo_config = get_mongodb_config(env)
        logger.info(f"MongoDB configuration for {env} environment:")
        # Log non-sensitive parts of the configuration
        for key in mongo_config:
            if key != 'host':  # Don't log the full host string as it may contain credentials
                logger.info(f"  {key}: {mongo_config[key]}")

        # Get Redis configuration
        redis_config = get_redis_config(env)
        logger.info(f"Redis configuration for {env} environment:")
        # Log non-sensitive parts of the configuration
        for key in redis_config:
            if key != 'password':  # Don't log the password
                logger.info(f"  {key}: {redis_config[key]}")

        # Initialize Spark
        logger.info(f"Initializing Spark session for {env} environment")
        spark = create_spark_session(env)
        logger.info(f"Spark session initialized with app ID: {spark.sparkContext.applicationId}")

        # Verify Spark is configured correctly
        logger.info("Checking Spark configuration:")
        if env == 'prod':
            logger.info("  Production spark configuration settings applied")
        else:
            logger.info("  Development spark configuration settings applied")

        # Initialize Redis
        logger.info(f"Connecting to Redis ({env} environment)")
        redis_client = setup_redis_connection(env)

        # Process data for specified year or current year
        process_data(spark, redis_client, year, env)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Job completed successfully at {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        # Print more detailed exception info for debugging
        import traceback
        logger.error(f"Exception details: {traceback.format_exc()}")
        sys.exit(1)

    finally:
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()
        if redis_client:
            logger.info("Closing Redis connection")
            redis_client.close()


if __name__ == "__main__":
    main()