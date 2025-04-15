from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pymongo import MongoClient, UpdateOne
import os
import sys
from datetime import datetime
import pytz
import time
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("StreamingCreateUserHandler")


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
            'monthly_db': 'activity_membershiptransactionmonthly_dev',
            'yearly_db': 'activity_membershiptransactionyearly_dev'
        }
    else:  # prod
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017',
            'monthly_db': 'activity_membershiptransactionmonthly',
            'yearly_db': 'activity_membershiptransactionyearly',
            'auth_source': 'admin'
        }


def get_kafka_config(env):
    """Return Kafka configuration for the specified environment"""
    if env == 'dev':
        return {
            'bootstrap.servers': '192.168.8.184:9092',
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN'
        }
    else:  # prod
        return {
            'bootstrap.servers': '192.168.11.201:9092,192.168.11.202:9092,192.168.11.203:9092',
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="3z740GCxK5xWfqoqKwxj";',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN'
        }


def create_spark_session(app_name, env):
    """Create Spark session with environment-specific configurations"""
    return SparkSession.builder \
        .appName(f"{app_name}-{env}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_mongodb_client(env):
    """Create and return MongoDB client based on environment"""
    mongo_config = get_mongodb_config(env)

    try:
        # Create MongoDB client with appropriate settings
        if env == 'dev':
            client = MongoClient(mongo_config['host'])
        else:  # prod
            client = MongoClient(
                mongo_config['host'],
                authSource=mongo_config['auth_source']
            )
        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise


def get_current_date():
    """Get current date information in Vietnam timezone"""
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)

    return {
        'month': now.month,
        'year': now.year,
        'timestamp': int(now.timestamp() * 1000),  # Milliseconds since epoch
        'datetime': now
    }


def process_create_user_events(batch_df, batch_id, env):
    """Process CreateUser events batch and upsert to MongoDB"""
    if batch_df.isEmpty():
        return

    try:
        # Filter for CreateUser events only
        create_user_df = batch_df.filter(col("event") == "CreateUser")

        # Count for logging
        event_count = create_user_df.count()
        logger.info(f"Processing batch {batch_id} with {event_count} CreateUser events in {env.upper()} environment")

        if event_count == 0:
            logger.info(f"No CreateUser events in batch {batch_id}")
            return

        # Get current date information
        current_date = get_current_date()
        current_month = current_date['month']
        current_year = current_date['year']
        current_timestamp = current_date['timestamp']

        # Extract user data - assuming datas field contains JSON array with user info
        user_df = create_user_df.select(
            explode(from_json(col("datas"), ArrayType(StringType()))).alias("user_data")
        )

        # Extract phone numbers from the user data
        # This would need specific parsing based on the actual message format
        # The approach below assumes we can extract phone directly from the message

        # Example extraction - adjust based on actual message format
        user_phones = []
        for row in create_user_df.collect():
            try:
                # Parse datas field which contains user info
                datas = json.loads(row["datas"]) if isinstance(row["datas"], str) else row["datas"]

                if isinstance(datas, list) and len(datas) > 0:
                    # Extract phone from first data item's User object if it exists
                    user_obj = datas[0].get('User', {})
                    phone = user_obj.get('Phone')

                    if phone:
                        user_phones.append(phone)
            except Exception as e:
                logger.error(f"Error parsing user data: {str(e)}")

        # If no valid phone numbers found, exit early
        if not user_phones:
            logger.warning(f"No valid phone numbers extracted from CreateUser events in batch {batch_id}")
            return

        logger.info(f"Extracted {len(user_phones)} phone numbers to process")

        # Initialize MongoDB client
        mongo_client = get_mongodb_client(env)
        mongo_config = get_mongodb_config(env)

        # Prepare collections
        monthly_db = mongo_client[mongo_config['monthly_db']]
        yearly_db = mongo_client[mongo_config['yearly_db']]

        monthly_collection_name = f"{current_year}_{current_month}"
        yearly_collection_name = f"{current_year}"

        # Ensure collections exist with indexes
        monthly_collection = monthly_db[monthly_collection_name]
        yearly_collection = yearly_db[yearly_collection_name]

        # Create indexes if they don't exist
        try:
            # Monthly collection indexes
            monthly_collection.create_index([("phone", 1)], unique=True)
            monthly_collection.create_index([("rank", 1)])
            monthly_collection.create_index([("totalpoints", -1)])

            # Yearly collection indexes
            yearly_collection.create_index([("phone", 1)], unique=True)
            yearly_collection.create_index([("rank", 1)])
            yearly_collection.create_index([("totalpoints", -1)])
        except Exception as e:
            logger.warning(f"Index creation warning: {str(e)}")

        # Prepare bulk operations
        monthly_ops = []
        yearly_ops = []

        # Default document values
        default_doc = {
            "totalpoints": 0,
            "rank": 9999,
            "membershipcode": "Level1",
            "membershipname": "Nhập môn",
            "timestamp": current_timestamp,
            "created_at": current_date['datetime'],
            "last_updated": current_date['datetime']
        }

        # Create batch operations for each phone
        for phone in user_phones:
            # Monthly collection update
            monthly_doc = default_doc.copy()
            monthly_doc.update({
                "phone": phone,
                "month": current_month,
                "year": current_year
            })

            monthly_ops.append(
                UpdateOne(
                    {"phone": phone, "month": current_month, "year": current_year},
                    {"$setOnInsert": monthly_doc},
                    upsert=True
                )
            )

            # Yearly collection update
            yearly_doc = default_doc.copy()
            yearly_doc.update({
                "phone": phone,
                "year": current_year
            })

            yearly_ops.append(
                UpdateOne(
                    {"phone": phone, "year": current_year},
                    {"$setOnInsert": yearly_doc},
                    upsert=True
                )
            )

        # Execute batch operations
        monthly_result = None
        yearly_result = None

        if monthly_ops:
            try:
                monthly_result = monthly_collection.bulk_write(monthly_ops, ordered=False)
                logger.info(f"Monthly collection update results - "
                            f"Matched: {monthly_result.matched_count}, "
                            f"Modified: {monthly_result.modified_count}, "
                            f"Upserted: {monthly_result.upserted_count}")
            except Exception as e:
                logger.error(f"Error updating monthly collection: {str(e)}")

        if yearly_ops:
            try:
                yearly_result = yearly_collection.bulk_write(yearly_ops, ordered=False)
                logger.info(f"Yearly collection update results - "
                            f"Matched: {yearly_result.matched_count}, "
                            f"Modified: {yearly_result.modified_count}, "
                            f"Upserted: {yearly_result.upserted_count}")
            except Exception as e:
                logger.error(f"Error updating yearly collection: {str(e)}")

        logger.info(f"Successfully processed batch {batch_id}")

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        raise
    finally:
        if 'mongo_client' in locals():
            mongo_client.close()


def main():
    # Determine environment
    env = get_environment()
    logger.info(f"Starting VML_Event streaming processor in {env.upper()} environment")

    # Get Kafka configuration for the environment
    kafka_config = get_kafka_config(env)

    # Initialize Spark Session
    spark = create_spark_session("vml-event-processor", env)

    # Define schema for VML_Event messages
    schema = StructType([
        StructField("Event", StringType(), True),
        StructField("Datas", StringType(), True)
    ])

    # Configure Kafka connection
    kafka_options = {
        "kafka.bootstrap.servers": kafka_config["bootstrap.servers"],
        "kafka.sasl.jaas.config": kafka_config["sasl.jaas.config"],
        "kafka.security.protocol": kafka_config["security.protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl.mechanism"],
        "subscribe": "VML_Event",
        "startingOffsets": "latest",  # Process only new events
        "kafka.group.id": f"vml-event-createuser-handler-{env}",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000"
    }

    # Read from Kafka stream
    event_stream_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Parse JSON and transform data - handle both legacy and newer formats
    parsed_df = event_stream_df.select(
        # Try to parse the value as a JSON object
        from_json(col("value").cast("string"), schema).alias("parsed_data")
    ).select(
        # Extract fields from the parsed JSON
        col("parsed_data.Event").alias("event"),
        col("parsed_data.Datas").alias("datas")
    )

    # Convert column names to lowercase for consistency
    for column in parsed_df.columns:
        parsed_df = parsed_df.withColumnRenamed(column, column.lower())

    # Customize process_batch function to include environment
    def process_batch_with_env(batch_df, batch_id):
        return process_create_user_events(batch_df, batch_id, env)

    # Environment-specific checkpoint location
    checkpoint_location = f"/activity_{env}/chk-point-dir/vml-event-createuser-handler"

    # Start streaming process
    streaming_query = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch_with_env) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='30 seconds') \
        .start()

    # Log query status
    logger.info(f"Streaming query started with ID: {streaming_query.id}")
    logger.info(f"Streaming from: {kafka_options['subscribe']}")
    logger.info(f"Using checkpoint location: {checkpoint_location}")

    # Wait for termination
    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()