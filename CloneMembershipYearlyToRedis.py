from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, lit, current_timestamp
import sys
import argparse
from datetime import datetime
import logging
import os
import time
import json

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
            'is_cluster': False,
            'host': '192.168.8.226',
            'port': 6379,
            'password': '0ef1sJm19w3OKHiH',
            'decode_responses': True,
            'index_name': 'activity-ranking-yearly-idx',
            'key_prefix': 'activity_membership_yearly:'
        }
    else:  # prod
        return {
            'is_cluster': True,
            'nodes': [
                {'host': '192.168.11.227', 'port': 6379},
                {'host': '192.168.11.228', 'port': 6379},
                {'host': '192.168.11.229', 'port': 6379}
            ],
            'password': '',  # Redis cluster password (empty if no password)
            'decode_responses': True,
            'index_name': 'activity-ranking-yearly-idx',
            'key_prefix': 'activity_membership_yearly:'
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
    try:
        # Lazy import Redis modules
        from redis import Redis
        from redis.cluster import RedisCluster, ClusterNode
        from redis.commands.search.field import TextField, NumericField, TagField
        from redis.commands.search.indexDefinition import IndexDefinition, IndexType

        redis_config = get_redis_config(env)

        # Check if we should use cluster mode
        if redis_config.get('is_cluster', False):
            # Create ClusterNode objects for cluster nodes
            cluster_nodes = [
                ClusterNode(host=node['host'], port=node['port'])
                for node in redis_config['nodes']
            ]

            logger.info(f"Connecting to Redis Cluster in {env} environment")
            logger.info(f"Cluster nodes: {', '.join([f'{node.host}:{node.port}' for node in cluster_nodes])}")

            # Create Redis Cluster client
            redis_client = RedisCluster(
                startup_nodes=cluster_nodes,
                password=redis_config['password'] if redis_config['password'] else None,
                decode_responses=redis_config['decode_responses'],
                skip_full_coverage_check=True  # More resilient to node failures
            )
        else:
            # Standalone Redis connection
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
        logger.info(f"Successfully connected to Redis{' Cluster' if redis_config.get('is_cluster', False) else ''}")

        return redis_client, redis_config
    except Exception as e:
        logger.error(
            f"Failed to connect to Redis{' Cluster' if env == 'prod' else ''}: {str(e)}")
        raise


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
            return False
        else:
            logger.error(f"Error checking index: {str(e)}")
            raise


def create_ranking_index(redis_client, redis_config):
    """Create RediSearch index for ActivityRankingYearly"""
    try:
        # Import necessary modules for RediSearch
        from redis.commands.search.field import TextField, NumericField, TagField
        from redis.commands.search.indexDefinition import IndexDefinition, IndexType

        index_name = redis_config['index_name']
        prefix = redis_config['key_prefix']

        # First try to check if index exists
        try:
            check_result = check_index_exists(redis_client, index_name, redis_config.get('env', 'dev'))
            if check_result:
                logger.info(f"Index {index_name} already exists, skipping creation")
                return
        except Exception as check_error:
            logger.warning(f"Error checking index existence: {str(check_error)}")
            # Continue with index creation attempt

        # Try to drop the index if it exists
        try:
            redis_client.ft(index_name).dropindex(delete_documents=False)
            logger.info(f"Dropped existing index: {index_name}")
        except Exception as drop_error:
            logger.info(f"Index drop skipped: {str(drop_error)}")
            # Continue with index creation

        # Define schema matching the C# model
        schema = (
            # Phone field made both searchable and sortable for flexible querying
            TextField("$.Phone", as_name="Phone", sortable=True),

            # Membership fields
            TagField("$.MembershipCode", as_name="MembershipCode"),
            TextField("$.MembershipName", as_name="MembershipName"),

            # Time period fields
            NumericField("$.Year", as_name="Year"),

            # Ranking fields with sorting enabled
            NumericField("$.Rank", as_name="Rank", sortable=True),
            NumericField("$.TotalPoints", as_name="TotalPoints", sortable=True),
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
  - MembershipCode (Tag)
  - MembershipName (Text)
  - Year (Numeric)
  - Rank (Numeric, Sortable)
  - TotalPoints (Numeric, Sortable)
""")

        # Test the index
        info = redis_client.ft(index_name).info()
        logger.info("\nIndex Info:")
        for key, value in info.items():
            logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Failed to create index: {str(e)}")
        raise


def generate_document_id(year, phone, prefix):
    """Generate Redis document ID following the index pattern"""
    return f"{prefix}{year}:{phone}"


def process_batch(batch_df, redis_client, redis_config):
    """Process a batch of records and upsert to Redis"""
    try:
        records = batch_df.collect()
        logger.info(f"Processing {len(records)} records in batch")

        # Get Redis configuration values
        is_cluster = redis_config.get('is_cluster', False)
        index_name = redis_config['index_name']
        key_prefix = redis_config['key_prefix']

        inserts = 0
        updates = 0
        errors = 0

        # Process in smaller sub-batches for better control with Redis Cluster
        sub_batch_size = 200 if is_cluster else 1000
        for i in range(0, len(records), sub_batch_size):
            sub_batch = records[i:i + sub_batch_size]

            # In cluster mode, we handle each record individually
            if is_cluster:
                sub_batch_inserts = 0
                sub_batch_errors = 0

                for record in sub_batch:
                    try:
                        # Generate document ID
                        doc_id = generate_document_id(record['year'], record['phone'], key_prefix)

                        # Create document structure
                        new_doc = {
                            "Id": doc_id,
                            "Phone": record['phone'],
                            "MembershipCode": record['membershipcode'],
                            "Year": record['year'],
                            "Rank": record['rank'],
                            "MembershipName": record['membershipname'],
                            "TotalPoints": record['totalpoints']
                        }

                        # Store in Redis
                        redis_client.json().set(doc_id, "$", new_doc)
                        redis_client.sadd(index_name, doc_id)
                        sub_batch_inserts += 1
                    except Exception as record_error:
                        logger.error(f"Error processing record {record['phone']}: {str(record_error)}")
                        sub_batch_errors += 1

                # Update counters
                inserts += sub_batch_inserts
                errors += sub_batch_errors

                # Log progress
                logger.info(
                    f"Processed sub-batch {i // sub_batch_size + 1}: {sub_batch_inserts} inserted, {sub_batch_errors} errors")
            else:
                # Use pipeline for non-cluster Redis for better performance
                pipeline = redis_client.pipeline(transaction=False)
                sub_batch_inserts = 0

                for record in sub_batch:
                    try:
                        # Generate document ID
                        doc_id = generate_document_id(record['year'], record['phone'], key_prefix)

                        # Create document structure
                        new_doc = {
                            "Id": doc_id,
                            "Phone": record['phone'],
                            "MembershipCode": record['membershipcode'],
                            "Year": record['year'],
                            "Rank": record['rank'],
                            "MembershipName": record['membershipname'],
                            "TotalPoints": record['totalpoints']
                        }

                        # Add to pipeline
                        pipeline.json().set(doc_id, "$", new_doc)
                        pipeline.sadd(index_name, doc_id)
                        sub_batch_inserts += 1
                    except Exception as record_error:
                        logger.error(f"Error preparing record {record['phone']}: {str(record_error)}")
                        errors += 1

                # Execute pipeline
                try:
                    pipeline.execute()
                    inserts += sub_batch_inserts
                    logger.info(f"Processed sub-batch {i // sub_batch_size + 1}: {sub_batch_inserts} inserted")
                except Exception as pipeline_error:
                    logger.error(f"Error executing pipeline: {str(pipeline_error)}")
                    errors += sub_batch_inserts

            # Add a small delay between batches to avoid overwhelming the Redis server
            time.sleep(0.1)

        logger.info(f"\nBatch processing summary:")
        logger.info(f"  Records processed: {len(records)}")
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
        # Using the MongoDB connector with the proper configuration already set in Spark session
        df = spark.read \
            .format("mongodb") \
            .option("database", db_name) \
            .option("collection", collection_name) \
            .load()

        # Log successful data load
        record_count = df.count()
        logger.info(f"Successfully loaded {record_count} records from MongoDB {db_name}.{collection_name}")
        return df

    except Exception as e:
        logger.error(f"Error reading from MongoDB: {str(e)}")
        # Provide more details to help diagnose connection issues
        logger.error(f"MongoDB connection details:")
        logger.error(f"  Database: {db_name}")
        logger.error(f"  Collection: {collection_name}")
        raise


def process_data(spark, redis_client, redis_config, year=None, env='dev'):
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
        batch_size = 500  # Smaller batch size for better control

        # Repartition the dataframe for more efficient processing
        # Safely calculate number of partitions
        num_partitions = max(1, min(20, (total_records + batch_size - 1) // batch_size))
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

            inserts, updates, errors = process_batch(batch_df, redis_client, redis_config)
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

        # Initialize Spark
        logger.info(f"Initializing Spark session for {env} environment")
        spark = create_spark_session(env)
        logger.info(f"Spark session initialized with app ID: {spark.sparkContext.applicationId}")

        # Initialize Redis connection
        logger.info(f"Connecting to Redis ({env} environment)")
        redis_client, redis_config = setup_redis_connection(env)
        redis_config['env'] = env  # Add env to redis_config for reference

        # Create or validate index
        logger.info(f"Setting up Redis search index")
        create_ranking_index(redis_client, redis_config)

        # Process data for specified year or current year
        process_data(spark, redis_client, redis_config, year, env)

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