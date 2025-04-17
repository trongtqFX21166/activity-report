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
    """Parse command line arguments with improved validation"""
    parser = argparse.ArgumentParser(description='Process daily driving reports and send events to Kafka')

    # Get yesterday's date as default
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    parser.add_argument('--date', type=str, default=yesterday,
                        help=f'Date to process in YYYY-MM-DD format (default: {yesterday})')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev',
                        help='Environment to use (dev or prod)')

    # Parse arguments
    try:
        args = parser.parse_args()

        # Parse and validate the date string
        try:
            process_date = datetime.strptime(args.date, '%Y-%m-%d')

            # Log the parsed parameters
            logger.info(f"Parsed arguments:")
            logger.info(f"  Date: {args.date} (parsed as: {process_date.strftime('%Y-%m-%d')})")
            logger.info(f"  Environment: {args.env.upper()}")

            return args.date, args.env, process_date
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Please use YYYY-MM-DD format.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error parsing arguments: {str(e)}")
        sys.exit(1)


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
            'hosts': ['192.168.8.180', '192.168.8.181', '192.168.8.182', '192.168.8.172'],  # Include all known nodes
            'port': 6379,
            'password': 'CDp4VrUuy744Ur2fGd5uakRsWe4Pu3P8',
            'prefix_key': 'vml',
            'decode_responses': True
        }
    else:  # prod
        return {
            'hosts': ['192.168.11.180', '192.168.11.181', '192.168.11.182'],  # Update with all prod nodes
            'port': 6379,
            'password': 'CDp4VrUuy744Ur2fGd5uakRsWe4Pu3P8',  # Use the same password or update for prod
            'prefix_key': 'vml',  # Assuming same prefix for prod
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
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.shuffle.service.enabled", "false") \
        .config("spark.python.worker.memory", "512m") \
        .config("spark.kryoserializer.buffer.max", "512m")

    return builder.enableHiveSupport().getOrCreate()


def extract_device_id(imei):
    """Extract numeric device ID from IMEI with VML_ prefix"""
    # Only process IMEIs that start with "VML_"
    if imei and isinstance(imei, str) and imei.startswith("VML_"):
        return imei[4:].lstrip("0")  # Remove prefix and leading zeros
    return None  # Return None for IMEIs that don't match the pattern


def get_user_info_from_redis(device_id, env):
    """Get user information from Redis using device ID

    Args:
        device_id: The numeric device ID (without VML_ prefix)
        env: Environment (dev or prod)

    Returns:
        Dict containing user information or None if not found
    """
    # Import locally to ensure it's available on executor nodes
    try:
        from redis import Redis
        import json
    except ImportError as e:
        logger.error(f"Failed to import Redis module: {str(e)}")
        logger.info("Attempting to install Redis module...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "redis"])
        from redis import Redis
        import json

    redis_config = get_redis_config(env)
    redis_client = None

    try:
        # Try each Redis host until we find one that works
        last_error = None
        for host in redis_config['hosts']:
            try:
                # Connect to a single Redis node
                redis_client = Redis(
                    host=host,
                    port=redis_config['port'],
                    password=redis_config['password'],
                    decode_responses=redis_config['decode_responses'],
                    socket_timeout=5.0  # Set a reasonable timeout
                )

                # Test connection
                redis_client.ping()

                # Get device information
                prefix = redis_config['prefix_key']
                device_key = f"{prefix}:data:User:{device_id}"

                # Log the key we're looking up for debugging
                logger.debug(f"Looking up Redis key: {device_key} on host {host}")

                device_info_str = redis_client.get(device_key)

                # If we get a MOVED error (not directly catchable), it will raise an exception
                # and we'll try the next host

                if device_info_str:
                    # Parse user information
                    try:
                        user_info = json.loads(device_info_str)
                        logger.debug(f"Successfully retrieved user info from host {host}")
                        return user_info
                    except json.JSONDecodeError as json_err:
                        logger.warning(f"Failed to parse user info as JSON: {str(json_err)}")
                        logger.debug(f"Raw user info content: {device_info_str[:100]}...")
                        return None
                else:
                    # Key not found on this host, try next one
                    logger.debug(f"Key not found on host {host}, trying next host if available")

                # Close this connection before trying the next host
                redis_client.close()
                redis_client = None

            except Exception as e:
                last_error = e
                logger.debug(f"Error connecting to Redis host {host}: {str(e)}")

                # If this is a MOVED error, we could parse the target host from the error message
                # But for simplicity, we'll just try all hosts in the list

                if redis_client:
                    try:
                        redis_client.close()
                    except:
                        pass
                    redis_client = None

        # If we got here, we tried all hosts and didn't find the key
        if last_error:
            logger.warning(f"Could not retrieve data for device {device_id} from any Redis host: {str(last_error)}")
        else:
            logger.debug(f"No user information found in Redis for device ID: {device_id}")
        return None

    except Exception as e:
        logger.error(f"Unexpected error getting user information from Redis for device ID {device_id}: {str(e)}")
        return None

    finally:
        if redis_client:
            # Close the Redis connection
            try:
                redis_client.close()
            except:
                pass


def read_daily_reports(spark, process_date, env):
    """Read daily summary reports from Cassandra for the specified date

    This function loads data for the given date with additional debugging info.
    """
    cassandra_config = get_cassandra_config(env)

    # Convert process_date to Unix timestamp (seconds since epoch) for the start of the day
    date_timestamp = int(process_date.timestamp())

    logger.info(
        f"Reading daily summary reports for date: {process_date.strftime('%Y-%m-%d')} (timestamp: {date_timestamp})")
    logger.info(f"Cassandra configuration: {cassandra_config}")

    try:
        # Read from Cassandra with more detailed logging
        logger.info(f"Attempting to connect to Cassandra: {cassandra_config['contact_points']}")
        logger.info(f"Keyspace: {cassandra_config['keyspace']}, Table: {cassandra_config['table']}")

        # Test if we can load any data before applying filters
        test_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=cassandra_config['table'], keyspace=cassandra_config['keyspace']) \
            .load() \
            .limit(5)

        # Check if we can successfully load any data
        test_count = test_df.count()
        logger.info(f"Test query retrieved {test_count} records from Cassandra")

        if test_count > 0:
            logger.info("Sample of data structure from Cassandra:")
            test_df.printSchema()

            # Now load the actual filtered data
            summary_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=cassandra_config['table'], keyspace=cassandra_config['keyspace']) \
                .load() \
                .filter((col("date") == date_timestamp) & (col("distance") >= 1000))

            # Cache the dataframe since we'll be using it multiple times
            summary_df.cache()

            # Get count after date filter
            date_filtered_count = summary_df.count()
            logger.info(f"Found {date_filtered_count} records for date {process_date.strftime('%Y-%m-%d')}")

            # Apply distance filter
            filtered_df = summary_df.filter(col("distance").isNotNull() & (col("distance") > 0))
            distance_filtered_count = filtered_df.count()

            # Log filtering results
            logger.info(f"After distance filter: {distance_filtered_count} records with distance > 0")
            logger.info(
                f"Filtered out {date_filtered_count - distance_filtered_count} records with distance ≤ 0 or NULL")

            # Additional debugging: check for any VML devices
            vml_count = filtered_df.filter(col("deviceimei").like("VML_%")).count()
            logger.info(f"Found {vml_count} records from VML devices")

            # Take a small sample for logging purposes
            sample_df = filtered_df.limit(5)
            logger.info("Sample data (up to 5 records):")
            sample_df.select("deviceimei", "date", "distance", "drivingtime", "month", "year").show(truncate=False)

            return filtered_df
        else:
            logger.warning("No data found in Cassandra table. Please verify connection and table exists.")
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

    except Exception as e:
        logger.error(f"Error reading daily summary reports: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

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
        # Check if reports dataframe is empty
        if reports_df.isEmpty():
            logger.warning("Reports dataframe is empty, no device mapping needed")
            # Return empty dataframe with expected schema
            schema = StructType([
                StructField("deviceimei", StringType(), True),
                StructField("phone", StringType(), True)
            ])
            return spark.createDataFrame([], schema)

        # Extract distinct VML device IMEIs from the reports DataFrame
        logger.info("Extracting distinct VML device IMEIs from daily reports")
        device_imeis_df = reports_df \
            .select("deviceimei") \
            .filter(col("deviceimei").like("VML_%")) \
            .distinct()

        # Count distinct VML devices
        vml_count = device_imeis_df.count()
        logger.info(f"Found {vml_count} distinct VML devices")

        if vml_count == 0:
            logger.warning("No VML devices found in the data")
            # Return empty dataframe with expected schema
            schema = StructType([
                StructField("deviceimei", StringType(), True),
                StructField("phone", StringType(), True)
            ])
            return spark.createDataFrame([], schema)

        # Extract device IDs using UDF
        extract_device_id_udf = udf(lambda imei: extract_device_id(imei), StringType())

        # Create DataFrame with device IDs
        device_ids_df = device_imeis_df \
            .withColumn("device_id", extract_device_id_udf(col("deviceimei"))) \
            .select("deviceimei", "device_id") \
            .filter(col("device_id").isNotNull())

        # Count valid device IDs
        valid_device_count = device_ids_df.count()
        logger.info(f"Extracted {valid_device_count} valid device IDs")

        if valid_device_count == 0:
            logger.warning("No valid device IDs could be extracted")
            # Return empty dataframe with expected schema
            schema = StructType([
                StructField("deviceimei", StringType(), True),
                StructField("phone", StringType(), True)
            ])
            return spark.createDataFrame([], schema)

        # Process in batches of 50 (reduced from 500 to avoid timeouts)
        # First collect to driver - this is safe because we're just working with the distinct IMEIs
        device_rows = device_ids_df.collect()

        # Create schema for result dataframe
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("phone", StringType(), True)
        ])

        # Initialize empty result
        result_rows = []
        batch_size = 50  # Reduced batch size
        total_devices = len(device_rows)

        # Debug limit - use the smaller of 50 or total_devices
        debug_limit = 50 if total_devices > 50 else total_devices  # Use direct comparison instead of min()

        logger.info(f"Processing {total_devices} VML devices in batches of {batch_size}")
        logger.info(f"Processing the first {debug_limit} devices")

        # Process in batches, but limit to debug_limit devices for debugging
        for i in range(0, debug_limit, batch_size):
            # Calculate end index for this batch ensuring we don't exceed debug_limit
            end_idx = i + batch_size
            if end_idx > debug_limit:
                end_idx = debug_limit

            batch = device_rows[i:end_idx]
            batch_size_actual = len(batch)

            # Calculate total batches
            total_batches = (debug_limit + batch_size - 1) // batch_size
            current_batch = i // batch_size + 1

            logger.info(f"Processing batch {current_batch} of {total_batches} ({batch_size_actual} devices)")

            # Process each device in the batch
            successful_lookups = 0
            for row in batch:
                device_imei = row["deviceimei"]
                device_id = row["device_id"]

                # Add some debug info
                logger.debug(f"Looking up device ID: {device_id} (from IMEI: {device_imei})")

                user_info = get_user_info_from_redis(device_id, env)

                if user_info and "Phone" in user_info:
                    result_rows.append((device_imei, user_info["Phone"]))
                    successful_lookups += 1

            # Log progress
            logger.info(f"Completed batch {current_batch}: {successful_lookups}/{batch_size_actual} successful lookups")
            logger.info(f"Total mapped so far: {len(result_rows)}/{debug_limit}")

            # Add a small delay between batches to avoid overwhelming Redis
            time.sleep(0.1)

        # Create dataframe from results
        if result_rows:
            device_mapping = spark.createDataFrame(result_rows, schema).cache()

            # Count the mappings found
            mapping_count = device_mapping.count()
            logger.info(f"Device mapping complete: {mapping_count} devices successfully mapped")

            # Show sample if available
            if mapping_count > 0:
                # Calculate sample count
                sample_count = 5
                logger.info(f"Sample of device-phone mappings (showing {sample_count} records):")
                device_mapping.limit(sample_count).show(truncate=False)

            return device_mapping
        else:
            logger.warning("No device mappings found - no phones could be matched to devices")
            return spark.createDataFrame([], schema)

    except Exception as e:
        logger.error(f"Error mapping devices to phones: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

        # Return empty dataframe with expected schema
        schema = StructType([
            StructField("deviceimei", StringType(), True),
            StructField("phone", StringType(), True)
        ])
        return spark.createDataFrame([], schema)


def prepare_kafka_events(reports_df, device_mapping):
    """Prepare Kafka events by joining reports with device mapping"""
    # Check if both dataframes have data
    if reports_df.isEmpty():
        logger.warning("Reports dataframe is empty, no events to prepare")
        return None

    if device_mapping.isEmpty():
        logger.warning("Device mapping dataframe is empty, no events to prepare")
        return None

    # Log the counts before joining
    reports_count = reports_df.count()
    mapping_count = device_mapping.count()
    logger.info(f"Preparing to join {reports_count} reports with {mapping_count} device mappings")

    # Join with device mapping to get phone numbers
    try:
        joined_df = reports_df.join(
            device_mapping,
            on="deviceimei",
            how="inner"
        )

        # Count results after join
        joined_count = joined_df.count()
        logger.info(f"Join result: {joined_count} records")

        if joined_count == 0:
            logger.warning("Join produced 0 records - no matching devices found")
            return None

        # Filter for records with distance > 0
        filtered_df = joined_df.filter(col("distance") > 0)

        # Count filtered records
        filtered_count = filtered_df.count()
        logger.info(f"Filtered out {joined_count - filtered_count} records with distance ≤ 0")
        logger.info(f"Keeping {filtered_count} records with distance > 0")

        if filtered_count == 0:
            logger.warning("No records with positive distance values")
            return None

        # Take a small sample to verify join worked
        sample_size = 3 if filtered_count > 3 else filtered_count
        sample_filtered = filtered_df.limit(sample_size)
        logger.info("Sample joined data:")
        sample_filtered.select("deviceimei", "phone", "distance", "drivingtime").show(truncate=False)

        # Generate UUID for event ID
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

        # Function to convert distance from m to km and round
        # Use a safer calculation
        meters_to_km_udf = udf(
            lambda meters: str(int(float(meters) / 1000)) if meters is not None else "0",
            StringType()
        )

        events_df = filtered_df.select(
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
            meters_to_km_udf(col("distance")).alias("Data")  # Convert distance from m to km and round
        )

        # Log conversion example
        logger.info("Distance conversion example (m to km):")
        filtered_df.select(
            col("deviceimei"),
            col("phone"),
            col("distance").alias("distance_meters"),
            (col("distance") / 1000).cast("float").alias("distance_km")
        ).limit(5).show()

        # Count final events
        events_count = events_df.count()
        logger.info(f"Generated {events_count} events ready to send to Kafka")

        return events_df

    except Exception as e:
        logger.error(f"Error preparing Kafka events: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def send_to_kafka(events_df, env):
    """Send events to Kafka using Spark's built-in Kafka integration"""
    if events_df is None or events_df.isEmpty():
        logger.warning("No events to send to Kafka")
        return 0

    try:
        # Get Kafka configuration
        kafka_config = get_kafka_config(env)
        logger.info(f"Sending events to Kafka topic: {kafka_config['topic']}")

        # Package events into the proper format for Kafka
        # Convert to array format first (wrapped in []) as expected by VML_ActivityEvent topic
        events_json_df = events_df.select(
            to_json(
                array(
                    struct(
                        col("Id"), col("Phone"), col("Date"), col("Month"), col("Year"),
                        col("TimeStamp"), col("MembershipCode"), col("Event"), col("ReportCode"),
                        col("EventName"), col("ReportName"), col("Data")
                    )
                )
            ).alias("value")
        )

        # Show sample of the JSON format
        logger.info("Sample JSON event format:")
        events_json_df.limit(1).show(truncate=False)

        # Configure Kafka producer options
        kafka_options = {
            "kafka.bootstrap.servers": kafka_config["bootstrap.servers"],
            "kafka.sasl.jaas.config": kafka_config["sasl.jaas.config"],
            "kafka.security.protocol": kafka_config["security.protocol"],
            "kafka.sasl.mechanism": kafka_config["sasl.mechanism"],
            "topic": kafka_config["topic"]
        }

        # Write to Kafka using Spark's built-in writer
        events_json_df.write \
            .format("kafka") \
            .options(**kafka_options) \
            .save()

        # Get count of events sent
        events_count = events_json_df.count()
        logger.info(f"Successfully sent {events_count} events to Kafka topic {kafka_config['topic']}")

        return events_count

    except Exception as e:
        logger.error(f"Error sending events to Kafka: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 0


def main():
    """Main execution function"""
    start_time = datetime.now()

    try:
        # Parse arguments
        date_str, env, process_date = parse_arguments()
        logger.info(f"Starting Daily Driving Report Job for date {date_str} in {env.upper()} environment")
        logger.info(f"Python version: {sys.version}")

        # Initialize Spark
        spark = create_spark_session("daily-driving-report", env)
        logger.info(f"Spark session created with app ID: {spark.sparkContext.applicationId}")

        # 1. Read filtered daily summary reports for the date
        all_reports_df = read_daily_reports(spark, process_date, env)

        # Early exit if no data
        if all_reports_df.isEmpty():
            logger.warning(f"No valid daily reports found for {date_str}. Job complete with no data to process.")
            spark.stop()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Job completed in {duration:.2f} seconds - no data processed")
            return

        # 2. Filter for VML devices only
        vml_reports_df = all_reports_df.filter(col("deviceimei").like("VML_%"))
        vml_count = vml_reports_df.count()
        logger.info(f"Filtered for VML devices only: {vml_count} records")

        # Early exit if no VML devices
        if vml_count == 0:
            logger.warning(f"No VML devices found in data for {date_str}. Job complete with no VML data to process.")
            spark.stop()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Job completed in {duration:.2f} seconds - no VML data processed")
            return

        # 3. Load device-to-phone mapping
        device_mapping = load_device_mapping(spark, vml_reports_df, env)

        # Early exit if no device mappings
        if device_mapping.isEmpty():
            logger.warning(f"No device mappings found for {date_str}. Job complete with no mapped devices.")
            spark.stop()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Job completed in {duration:.2f} seconds - no device mappings")
            return

        # 4. Prepare Kafka events
        events_df = prepare_kafka_events(vml_reports_df, device_mapping)

        # Early exit if no events
        if events_df is None or events_df.isEmpty():
            logger.warning(f"No events generated for {date_str}. Job complete with no events to send.")
            spark.stop()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Job completed in {duration:.2f} seconds - no events to send")
            return

        # 5. Send events to Kafka
        sent_count = send_to_kafka(events_df, env)

        # Log final results
        spark.stop()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Job completed in {duration:.2f} seconds")
        logger.info(f"Sent {sent_count} events to Kafka")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if 'spark' in locals() and spark:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except:
                pass


if __name__ == "__main__":
    main()