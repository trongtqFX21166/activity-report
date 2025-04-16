#!/usr/bin/env python3
"""
DailyDrivingReportJob.py - Processes daily driving data and sends events to VML_ActivityEvent topic

This job reads DailySummaryReport data from Cassandra for a specified date
and sends events to the VML_ActivityEvent Kafka topic for each user with driving activity.

Usage:
    spark-submit DailyDrivingReportJob.py --date YYYY-MM-DD --env [dev|prod]

Parameters:
    --date : Date to process in YYYY-MM-DD format (default: yesterday)
    --env  : Environment to use (dev or prod), defaults to 'dev'
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
import uuid
import json
import time
from datetime import datetime, timedelta
import logging
import os
from redis import Redis
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DailyDrivingReportJob")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process daily driving reports and send events to Kafka')

    # Get yesterday's date as default
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    parser.add_argument('--date', type=str, default=yesterday,
                        help=f'Date to process in YYYY-MM-DD format (default: {yesterday})')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev',
                        help='Environment to use (dev or prod)')

    args = parser.parse_args()

    # Parse the date string to validate format
    try:
        process_date = datetime.strptime(args.date, '%Y-%m-%d')
        return args.date, args.env, process_date
    except ValueError as e:
        logger.error(f"Invalid date format: {args.date}. Please use YYYY-MM-DD format.")
        raise


def get_cassandra_config(env):
    """Return Cassandra configuration for the specified environment"""
    if env == 'dev':
        return {
            'contact_points': "192.168.8.165,192.168.8.166,192.168.8.183",
            'keyspace': 'iothub',
            'table': 'dailysummaryreport'
        }
    else:  # prod
        return {
            'contact_points': "192.168.11.165,192.168.11.166,192.168.11.183",
            'keyspace': 'iothub',
            'table': 'dailysummaryreport'
        }


def get_kafka_config(env):
    """Return Kafka configuration for the specified environment"""
    if env == 'dev':
        return {
            'bootstrap.servers': '192.168.8.184:9092',
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'topic': 'VML_ActivityEvent'
        }
    else:  # prod
        return {
            'bootstrap.servers': '192.168.11.201:9092,192.168.11.202:9092,192.168.11.203:9092',
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="3z740GCxK5xWfqoqKwxj";',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'topic': 'VML_ActivityEvent'
        }


def get_redis_config(env):
    """Return Redis configuration for the specified environment"""
    if env == 'dev':
        return {
            'hosts': '192.168.8.180,192.168.8.181,192.168.8.182',
            'password': 'CDp4VrUuy744Ur2fGd5uakRsWe4Pu3P8',
            'prefix_key': 'vml',
            'decode_responses': True
        }
    else:  # prod
        return {
            'hosts': '192.168.11.180,192.168.11.181,192.168.11.182',
            'password': 'CDp4VrUuy744Ur2fGd5uakRsWe4Pu3P8',  # Use the same password or update for prod
            'prefix_key': 'vml',  # Assuming different prefix for prod
            'decode_responses': True
        }


def create_spark_session(app_name, env):
    """Create Spark session with environment-specific configurations"""
    cassandra_config = get_cassandra_config(env)

    builder = SparkSession.builder \
        .appName(f"{app_name}-{env}") \
        .config("spark.cassandra.connection.host", cassandra_config['contact_points']) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.driver.extraClassPath",
                "/opt/conda/lib/python3.8/site-packages/pyspark/jars/kafka-clients-3.3.1.jar") \
        .config("spark.executor.extraClassPath",
                "/opt/conda/lib/python3.8/site-packages/pyspark/jars/kafka-clients-3.3.1.jar") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.shuffle.service.enabled", "false") \
        .config("spark.python.worker.memory", "512m") \
        .config("spark.kryoserializer.buffer.max", "512m")

    return builder.enableHiveSupport().getOrCreate()


def get_user_info_from_redis(device_id, env):
    """Get user information from Redis using device ID

    Args:
        device_id: The numeric device ID (without VML_ prefix)
        env: Environment (dev or prod)

    Returns:
        Dict containing user information or None if not found
    """
    from redis import Redis
    import json

    redis_config = get_redis_config(env)

    try:
        # Parse hosts
        hosts = redis_config['hosts'].split(',')
        host = hosts[0]  # Use the first host from the list

        # Connect to Redis
        redis_client = Redis(
            host=host,
            port=6379,  # Default port
            password=redis_config['password'],
            decode_responses=redis_config['decode_responses']
        )

        # Get device information
        prefix = redis_config['prefix_key']
        device_key = f"{prefix}:data:User:{device_id}"
        device_info_str = redis_client.get(device_key)

        if not device_info_str:
            logger.warning(f"No user information found in Redis for user ID: {device_id} (key: {user_key})")
            return None

        # Parse user information
        user_info = json.loads(device_info_str)

        return user_info
    except Exception as e:
        logger.error(f"Error getting user information from Redis: {str(e)}")
        return None
    finally:
        if 'redis_client' in locals():
            redis_client.close()


def extract_device_id(imei):
    """Extract numeric device ID from IMEI with VML_ prefix"""
    # Only process IMEIs that start with "VML_"
    if imei and isinstance(imei, str) and imei.startswith("VML_"):
        return imei[4:].lstrip("0")  # Remove prefix and leading zeros
    return None  # Return None for IMEIs that don't match the pattern


def read_all_daily_reports(spark, process_date, env):
    """Read ALL daily summary reports from Cassandra for the specified date

    This function loads the complete dataset for the given date without any filtering
    to ensure we have a complete picture of the data for analysis purposes.
    """
    cassandra_config = get_cassandra_config(env)

    # Convert process_date to Unix timestamp (seconds since epoch) for the start of the day
    date_timestamp = int(process_date.timestamp())

    logger.info(
        f"Reading ALL daily summary reports for date: {process_date.strftime('%Y-%m-%d')} (timestamp: {date_timestamp})")

    try:
        # Read from Cassandra - only filter by date, no IMEI filtering at this stage
        summary_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=cassandra_config['table'], keyspace=cassandra_config['keyspace']) \
            .load() \
            .filter(col("date") == date_timestamp)

        # Cache the dataframe since we'll be using it multiple times
        summary_df.cache()

        # Avoid using count() for performance reasons - just log that we're reading the data
        logger.info(f"Successfully loaded all daily summary reports for date {process_date.strftime('%Y-%m-%d')}")

        # Take a small sample for logging purposes without counting the entire dataset
        sample_df = summary_df.limit(5)
        logger.info("Sample data (up to 5 records):")
        sample_df.select("deviceimei", "date", "distance", "drivingtime", "month", "year").show(truncate=False)

        return summary_df
    except Exception as e:
        logger.error(f"Error reading daily summary reports: {str(e)}")
        # Return empty dataframe with expected schema
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("date", LongType(), True),
            StructField("distance", IntegerType(), True),
            StructField("drivingtime", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("year", IntegerType(), True)
        ])
        return spark.createDataFrame([], schema)


def load_device_mapping(spark, reports_df, env):
    """Map device IMEIs to phone numbers using Redis

    Args:
        spark: SparkSession
        reports_df: DataFrame containing the reports
        env: Environment (dev or prod)

    Returns:
        DataFrame with deviceimei and phone columns
    """
    try:
        # Extract distinct VML device IMEIs from the reports DataFrame
        logger.info("Extracting distinct VML device IMEIs from daily reports")
        device_imeis_df = reports_df \
            .select("deviceimei") \
            .filter(col("deviceimei").startswith("VML_")) \
            .distinct()

        # Extract device IDs using UDF
        extract_device_id_udf = udf(lambda imei: extract_device_id(imei), StringType())

        # Create DataFrame with device IDs
        device_ids_df = device_imeis_df \
            .withColumn("device_id", extract_device_id_udf(col("deviceimei"))) \
            .select("deviceimei", "device_id") \
            .filter(col("device_id").isNotNull())

        # Process in batches of 500
        # First collect to driver - this is safe because we're just working with the distinct IMEIs
        device_rows = device_ids_df.collect()

        # Create schema for result dataframe
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("phone", StringType(), True)
        ])

        # Initialize empty result
        result_rows = []
        batch_size = 500
        total_devices = len(device_rows)

        logger.info(f"Processing {total_devices} VML devices in batches of {batch_size}")

        # Process in batches
        for i in range(0, total_devices, batch_size):
            batch = device_rows[i:i + batch_size]
            logger.info(
                f"Processing batch {i // batch_size + 1} of {(total_devices + batch_size - 1) // batch_size} ({len(batch)} devices)")

            # Process each device in the batch
            for row in batch:
                device_imei = row["deviceimei"]
                device_id = row["device_id"]

                user_info = get_user_info_from_redis(device_id, env)

                if user_info and "Phone" in user_info:
                    result_rows.append((device_imei, user_info["Phone"]))

            # Log progress
            logger.info(f"Completed batch {i // batch_size + 1}, total mapped so far: {len(result_rows)}")

        # Create dataframe from results
        device_mapping = spark.createDataFrame(result_rows, schema).cache()

        # Take a small sample to verify data without counting
        sample_mapping = device_mapping.limit(5)
        sample_count = sample_mapping.count()  # This is a small count, so performance impact is minimal

        logger.info(f"Sample of device-phone mappings (showing {sample_count} records):")
        sample_mapping.show(truncate=False)

        return device_mapping
    except Exception as e:
        logger.error(f"Error mapping devices to phones: {str(e)}")
        # Return empty dataframe with expected schema
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("phone", StringType(), True)
        ])
        return spark.createDataFrame([], schema)


def prepare_kafka_events(reports_df, device_mapping):
    """Prepare Kafka events by joining reports with device mapping"""
    # Join with device mapping to get phone numbers
    joined_df = reports_df.join(
        device_mapping,
        on="deviceimei",
        how="inner"
    )

    # Take a small sample to verify join worked without counting entire dataset
    sample_joined = joined_df.limit(3)
    sample_joined.show(truncate=False)

    # Generate UUID for event ID
    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

    events_df = joined_df.select(
        uuid_udf().alias("Id"),
        col("phone").alias("Phone"),
        col("date").alias("Date"),
        col("month").alias("Month"),
        col("year").alias("Year"),
        col("date").alias("TimeStamp"),  # Use the same timestamp as Date
        lit("Level1").alias("MembershipCode"),
        lit("DrivingDaily").alias("Event"),
        lit("DrivingDaily").alias("ReportCode"),
        lit("Lái xe hàng ngày").alias("EventName"),
        lit("Lái xe hàng ngày").alias("ReportName"),
        col("distance").cast("string").alias("Data")  # Convert distance to string for Data field
    )

    return events_df


def send_to_kafka(events_df, env):
    """Send events to Kafka in batches of 500"""
    if events_df is None:
        logger.warning("No events to send to Kafka")
        return 0

    kafka_config = get_kafka_config(env)

    # Take a small sample to verify event format
    sample_events = events_df.limit(1)
    sample_count = sample_events.count()  # Small count, minimal performance impact

    if sample_count == 0:
        logger.warning("No events to send to Kafka (empty dataset)")
        return 0

    logger.info(f"Preparing to send events to Kafka topic {kafka_config['topic']}")

    # Convert events to the expected JSON format for Kafka
    events_json_df = events_df.select(
        to_json(struct(
            col("Id"), col("Phone"), col("Date"), col("Month"), col("Year"),
            col("TimeStamp"), col("MembershipCode"), col("Event"), col("ReportCode"),
            col("EventName"), col("ReportName"), col("Data")
        )).alias("value")
    )

    # Show a sample of the JSON format
    logger.info("Sample JSON event format:")
    events_json_df.limit(1).show(truncate=False)

    try:
        # Collect data to driver - this is safe as we're only working with batches
        event_messages = events_json_df.collect()
        total_events = len(event_messages)
        batch_size = 500

        logger.info(f"Processing {total_events} events in batches of {batch_size}")

        # Process in batches
        total_sent = 0
        for i in range(0, total_events, batch_size):
            batch = event_messages[i:i + batch_size]
            batch_size_actual = len(batch)

            logger.info(
                f"Processing batch {i // batch_size + 1} of {(total_events + batch_size - 1) // batch_size} ({batch_size_actual} events)")

            # Send this batch using Kafka Python client
            from kafka import KafkaProducer
            import json

            # Create Kafka producer for this batch
            producer = KafkaProducer(
                bootstrap_servers=kafka_config["bootstrap.servers"].split(','),
                security_protocol=kafka_config["security.protocol"],
                sasl_mechanism=kafka_config["sasl.mechanism"],
                sasl_plain_username="admin",  # Extract from JAAS config
                sasl_plain_password=kafka_config["sasl.jaas.config"].split('password="')[1].split('";')[0]
            )

            # Process each message in the batch
            batch_sent = 0
            batch_errors = 0

            for record in batch:
                try:
                    # Send message to Kafka
                    producer.send(
                        kafka_config["topic"],
                        value=record["value"].encode('utf-8')
                    )
                    batch_sent += 1
                except Exception as e:
                    batch_errors += 1
                    logger.error(f"Error sending message to Kafka: {str(e)}")

            # Flush producer to ensure all messages are sent
            producer.flush()

            # Close the producer
            producer.close()

            total_sent += batch_sent

            logger.info(f"Batch {i // batch_size + 1} complete: Sent {batch_sent} events, {batch_errors} errors")

        logger.info(f"Successfully sent {total_sent} events to Kafka topic {kafka_config['topic']}")
        return total_sent
    except Exception as e:
        logger.error(f"Error sending events to Kafka: {str(e)}")
        return 0


def main():
    """Main execution function"""
    start_time = datetime.now()

    try:
        # Parse arguments
        date_str, env, process_date = parse_arguments()
        logger.info(f"Starting Daily Driving Report Job for date {date_str} in {env.upper()} environment")

        # Initialize Spark
        spark = create_spark_session("daily-driving-report", env)

        # 1. Read ALL daily summary reports for the date first
        # (including non-VML devices to get a complete picture)
        all_reports_df = read_all_daily_reports(spark, process_date, env)

        # 2. Filter for VML devices only
        vml_reports_df = all_reports_df.filter(col("deviceimei").startswith("VML_"))
        logger.info("Filtered for VML devices only")

        # 3. Load device-to-phone mapping
        device_mapping = load_device_mapping(spark, vml_reports_df, env)

        # 4. Prepare Kafka events
        events_df = prepare_kafka_events(vml_reports_df, device_mapping)

        # 5. Send events to Kafka
        sent_count = send_to_kafka(events_df, env)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Job completed in {duration:.2f} seconds")
        logger.info(f"Sent {sent_count} events to Kafka")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()