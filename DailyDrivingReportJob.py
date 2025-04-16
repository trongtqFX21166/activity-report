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
            'host': '192.168.8.180',  # Primary host
            'port': 6379,
            'password': 'CDp4VrUuy744Ur2fGd5uakRsWe4Pu3P8',
            'prefix_key': 'vml',
            'decode_responses': True
        }
    else:  # prod
        return {
            'host': '192.168.11.180',  # Primary host
            'port': 6379,
            'password': 'CDp4VrUuy744Ur2fGd5uakRsWe4Pu3P8',
            'prefix_key': 'vml',
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
        .config("spark.driver.memory", "4g")

    return builder.enableHiveSupport().getOrCreate()


def get_redis_connection(env):
    """Create Redis connection using environment configuration"""
    # Import here to ensure availability
    from redis import Redis

    config = get_redis_config(env)
    try:
        redis_client = Redis(
            host=config['host'],
            port=config['port'],
            password=config['password'],
            decode_responses=config['decode_responses'],
            socket_timeout=5.0
        )
        # Test connection
        redis_client.ping()
        logger.info(f"Successfully connected to Redis ({env} environment)")
        return redis_client
    except Exception as e:
        logger.error(f"Error connecting to Redis: {str(e)}")
        raise


def get_user_info_from_redis(device_id, redis_client):
    """Get user information from Redis using device ID and an existing connection"""
    import json

    try:
        # Get device information using the existing Redis connection
        prefix = get_redis_config(get_environment())['prefix_key']
        device_key = f"{prefix}:data:User:{device_id}"

        # Get the data
        device_info_str = redis_client.get(device_key)

        if device_info_str:
            # Parse user information
            user_info = json.loads(device_info_str)
            return user_info
        else:
            logger.debug(f"No user information found for device ID: {device_id}")
            return None

    except Exception as e:
        logger.error(f"Error getting user information for device ID {device_id}: {str(e)}")
        return None


def extract_device_id(imei):
    """Extract numeric device ID from IMEI with VML_ prefix"""
    if imei and isinstance(imei, str) and imei.startswith("VML_"):
        return imei[4:].lstrip("0")  # Remove prefix and leading zeros
    return None


def get_environment():
    """Get the current environment from command line arguments or environment variables"""
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() in ['dev', 'prod']:
        return sys.argv[1].lower()

    # Check environment variables
    env = os.environ.get('ENVIRONMENT', 'dev').lower()
    if env in ['dev', 'prod']:
        return env

    # Default to dev
    return 'dev'


def get_current_datetime_epoch():
    """Get current date and time information in seconds since epoch"""
    now = datetime.now()
    timestamp = int(time.time())
    date_timestamp = int(datetime(now.year, now.month, now.day).timestamp())

    return {
        "timestamp": timestamp,
        "date_timestamp": date_timestamp,
        "month": now.month,
        "year": now.year
    }


def read_daily_reports(spark, process_date, env):
    """Read daily summary reports from Cassandra for the specified date"""
    cassandra_config = get_cassandra_config(env)

    # Convert process_date to Unix timestamp (seconds since epoch) for the start of the day
    date_timestamp = int(process_date.timestamp())

    logger.info(
        f"Reading daily summary reports for date: {process_date.strftime('%Y-%m-%d')} (timestamp: {date_timestamp})")

    try:
        # Read from Cassandra - filter by date and for VML devices only
        summary_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=cassandra_config['table'], keyspace=cassandra_config['keyspace']) \
            .load() \
            .filter((col("date") == date_timestamp) &
                    (col("deviceimei").startswith("VML_")) &
                    (col("distance") > 1000))  # Only include meaningful distances (> 1km)

        # Count records for logging
        record_count = summary_df.count()
        logger.info(
            f"Found {record_count} VML device reports with distance > 1km for {process_date.strftime('%Y-%m-%d')}")

        # Show sample data
        if record_count > 0:
            logger.info("Sample data:")
            summary_df.select("deviceimei", "date", "distance", "drivingtime", "month", "year").show(5, truncate=False)

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


def map_devices_to_phones(reports_df, env):
    """Map device IMEIs to phone numbers using Redis"""
    try:
        # Extract device IDs using UDF
        extract_device_id_udf = udf(lambda imei: extract_device_id(imei), StringType())

        # Extract distinct VML device IMEIs and their IDs
        device_ids_df = reports_df \
            .select("deviceimei") \
            .distinct() \
            .withColumn("device_id", extract_device_id_udf(col("deviceimei"))) \
            .filter(col("device_id").isNotNull())

        # Count devices
        device_count = device_ids_df.count()
        logger.info(f"Found {device_count} distinct VML devices to map")

        if device_count == 0:
            logger.warning("No valid device IDs found to map")
            # Return empty dataframe with expected schema
            schema = StructType([
                StructField("deviceimei", StringType(), True),
                StructField("phone", StringType(), True)
            ])
            return spark.createDataFrame([], schema)

        # Collect device IDs to driver for Redis lookups
        device_rows = device_ids_df.collect()

        # Get Redis connection
        redis_client = get_redis_connection(env)

        # Process devices in batches
        result_rows = []
        batch_size = 100

        logger.info(f"Processing {device_count} VML devices in batches of {batch_size}")

        # Process in batches
        for i in range(0, device_count, batch_size):
            batch = device_rows[i:i + batch_size]
            batch_size_actual = len(batch)
            logger.info(
                f"Processing batch {i // batch_size + 1}/{(device_count + batch_size - 1) // batch_size} ({batch_size_actual} devices)")

            # Process each device in the batch
            successful_lookups = 0
            for row in batch:
                device_imei = row["deviceimei"]
                device_id = row["device_id"]

                user_info = get_user_info_from_redis(device_id, redis_client)

                if user_info and "Phone" in user_info:
                    result_rows.append((device_imei, user_info["Phone"]))
                    successful_lookups += 1

            # Log progress
            logger.info(
                f"Completed batch {i // batch_size + 1}: {successful_lookups}/{batch_size_actual} successful lookups")

            # Add a small delay between batches to avoid overwhelming Redis
            time.sleep(0.1)

        # Close Redis connection
        redis_client.close()

        # Create dataframe from results
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("phone", StringType(), True)
        ])

        device_mapping = spark.createDataFrame(result_rows, schema)

        # Log mapping results
        mapping_count = device_mapping.count()
        logger.info(
            f"Device mapping complete: {mapping_count}/{device_count} devices successfully mapped to phone numbers")

        # Show sample mapping
        if mapping_count > 0:
            logger.info("Sample device-phone mappings:")
            device_mapping.show(5, truncate=False)

        return device_mapping
    except Exception as e:
        logger.error(f"Error mapping devices to phones: {str(e)}")
        if 'redis_client' in locals():
            redis_client.close()

        # Return empty dataframe with expected schema
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("phone", StringType(), True)
        ])
        return spark.createDataFrame([], schema)


def create_event_messages(reports_df, device_mapping):
    """Create event messages for Kafka by joining reports with device mapping"""
    # Join reports with device mapping
    joined_df = reports_df.join(
        device_mapping,
        on="deviceimei",
        how="inner"
    )

    # Check join results
    join_count = joined_df.count()
    logger.info(f"Joined {join_count} reports with device mappings")

    if join_count == 0:
        logger.warning("No reports could be joined with phones")
        return None

    # Generate UUID for event ID
    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

    # Convert distance from meters to kilometers
    meters_to_km_udf = udf(
        lambda meters: int(round(float(meters) / 1000)) if meters is not None else 0,
        IntegerType()
    )

    # Get current datetime info
    dt_info = get_current_datetime_epoch()

    # Create event messages
    events_df = joined_df.select(
        uuid_udf().alias("Id"),
        col("phone").alias("Phone"),
        col("date").alias("Date"),
        col("month").alias("Month"),
        col("year").alias("Year"),
        lit(dt_info["timestamp"]).alias("TimeStamp"),
        lit("Level1").alias("MembershipCode"),
        lit("DrivingReport").alias("Event"),
        lit("").alias("ReportCode"),
        lit("Báo cáo lái xe").alias("EventName"),
        lit("Báo cáo lái xe hàng ngày").alias("ReportName"),
        # Format Data field as JSON string with distance in km
        to_json(struct(
            meters_to_km_udf(col("distance")).alias("DistanceKm"),
            col("drivingtime").alias("DrivingTimeSeconds")
        )).alias("Data")
    )

    # Show sample events
    logger.info("Sample event messages:")
    events_df.show(5, truncate=False)

    return events_df


def send_events_to_kafka(events_df, env):
    """Send event messages to Kafka"""
    if events_df is None or events_df.isEmpty():
        logger.warning("No events to send to Kafka")
        return 0

    kafka_config = get_kafka_config(env)

    # Count events
    event_count = events_df.count()
    logger.info(f"Preparing to send {event_count} events to Kafka topic {kafka_config['topic']}")

    # Convert events to array format expected by VML_ActivityEvent topic
    events_array_schema = ArrayType(StructType([
        StructField("Id", StringType(), True),
        StructField("Phone", StringType(), True),
        StructField("Date", LongType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Year", IntegerType(), True),
        StructField("TimeStamp", LongType(), True),
        StructField("MembershipCode", StringType(), True),
        StructField("Event", StringType(), True),
        StructField("ReportCode", StringType(), True),
        StructField("EventName", StringType(), True),
        StructField("ReportName", StringType(), True),
        StructField("Data", StringType(), True)
    ]))

    # Group events into batches to send as arrays
    batch_size = 10

    # Process in batches using foreachPartition
    def send_partition(partition_iterator):
        from kafka import KafkaProducer

        # Create Kafka producer
        producer = None
        try:
            # Extract credentials from JAAS config
            sasl_username = "admin"
            sasl_password = kafka_config["sasl.jaas.config"].split('password="')[1].split('";')[0]

            # Create producer
            producer = KafkaProducer(
                bootstrap_servers=kafka_config["bootstrap.servers"].split(','),
                security_protocol=kafka_config["security.protocol"],
                sasl_mechanism=kafka_config["sasl.mechanism"],
                sasl_plain_username=sasl_username,
                sasl_plain_password=sasl_password,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            # Collect records from partition
            records = list(partition_iterator)
            total_records = len(records)
            sent_count = 0

            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]

                # Convert batch to array format
                event_array = []
                for record in batch:
                    event_array.append({
                        "Id": record["Id"],
                        "Phone": record["Phone"],
                        "Date": record["Date"],
                        "Month": record["Month"],
                        "Year": record["Year"],
                        "TimeStamp": record["TimeStamp"],
                        "MembershipCode": record["MembershipCode"],
                        "Event": record["Event"],
                        "ReportCode": record["ReportCode"],
                        "EventName": record["EventName"],
                        "ReportName": record["ReportName"],
                        "Data": record["Data"]
                    })

                # Send batch to Kafka
                producer.send(kafka_config["topic"], event_array)
                sent_count += len(batch)

            # Flush and close producer
            producer.flush()
            logger.info(f"Sent {sent_count} events from partition to Kafka")

        except Exception as e:
            logger.error(f"Error sending events to Kafka: {str(e)}")
        finally:
            if producer:
                producer.close()

    # Use foreachPartition to send events efficiently
    events_df.foreachPartition(send_partition)

    logger.info(f"Completed sending events to Kafka topic {kafka_config['topic']}")
    return event_count


def main():
    """Main execution function"""
    start_time = datetime.now()

    try:
        # Parse arguments
        date_str, env, process_date = parse_arguments()
        logger.info(f"Starting Daily Driving Report Job for date {date_str} in {env.upper()} environment")

        # Initialize Spark
        spark = create_spark_session("daily-driving-report", env)

        # 1. Read daily reports for VML devices
        reports_df = read_daily_reports(spark, process_date, env)

        # Check if we have data to process
        if reports_df.isEmpty():
            logger.info(f"No driving reports found for {date_str}. Job completed.")
            return

        # 2. Map device IMEIs to phone numbers
        device_mapping = map_devices_to_phones(reports_df, env)

        # 3. Create event messages
        events_df = create_event_messages(reports_df, device_mapping)

        if events_df is None or events_df.isEmpty():
            logger.info("No event messages could be created. Job completed.")
            return

        # 4. Send events to Kafka
        send_events_to_kafka(events_df, env)

        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Job completed successfully in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()