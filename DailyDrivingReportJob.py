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
                .filter(col("date") == date_timestamp)

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

        # Debug limit - use the smaller of 10 or total_devices
        debug_limit = 10 if total_devices > 10 else total_devices

        logger.info(f"Processing {total_devices} VML devices in batches of {batch_size}")
        logger.info(f"DEBUGGING MODE: Only processing the first {debug_limit} devices")

        # Process in batches, but limit to debug_limit devices for debugging
        for i in range(0, debug_limit, batch_size):
            # Calculate end index for this batch ensuring we don't exceed debug_limit
            end_idx = i + batch_size
            if end_idx > debug_limit:
                end_idx = debug_limit

            batch = device_rows[i:end_idx]
            batch_size_actual = len(batch)

            # Calculate total batches without using min()
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
                # Calculate sample count without using min()
                sample_count = 5 if mapping_count > 5 else mapping_count
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
        # Calculate sample size without using min()
        sample_size = 3 if filtered_count > 3 else filtered_count
        sample_filtered = filtered_df.limit(sample_size)
        logger.info("Sample joined data:")
        sample_filtered.select("deviceimei", "phone", "distance", "drivingtime").show(truncate=False)

        # Generate UUID for event ID
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

        # Function to convert distance from m to km and round
        # FIX: Properly handle the float result by converting it to a string directly in the UDF
        meters_to_km_udf = udf(
            lambda meters: str(int(round(float(meters) / 1000))) if meters is not None else "0",
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
        # FIX: Use lit() here to properly wrap values in column objects
        filtered_df.select(
            col("deviceimei"),
            col("phone"),
            col("distance").alias("distance_meters"),
            (col("distance") / lit(1000)).cast("float").alias("distance_km")
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
    """Send events to Kafka in batches of 500"""
    if events_df is None or events_df.isEmpty():
        logger.warning("No events to send to Kafka")
        return 0

    kafka_config = get_kafka_config(env)
    logger.info(f"Kafka configuration: {kafka_config['bootstrap.servers']}, Topic: {kafka_config['topic']}")

    # Take a small sample to verify event format
    sample_events = events_df.limit(1)
    try:
        sample_count = sample_events.count()  # Small count, minimal performance impact
    except Exception as e:
        logger.error(f"Error counting sample events: {str(e)}")
        sample_count = 0

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

    # Show a sample of the JSON format - wrap in try-except to avoid failures
    try:
        logger.info("Sample JSON event format:")
        events_json_df.limit(1).show(truncate=False)
    except Exception as e:
        logger.warning(f"Failed to show sample JSON event: {str(e)}")

    try:
        # FIX: Use the full import path for KafkaProducer
        try:
            # Try importing from kafka package first
            import kafka
            logger.info(f"Kafka package version: {kafka.__version__}")

            # Try direct import from kafka.producer first
            try:
                from kafka.producer import KafkaProducer
                logger.info("Successfully imported KafkaProducer from kafka.producer")
            except ImportError:
                # Fall back to install kafka-python explicitly
                logger.warning("Cannot import KafkaProducer. Installing kafka-python...")
                import subprocess
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", "--force-reinstall", "kafka-python==2.0.2"])
                # Try the import again
                from kafka.producer import KafkaProducer
                logger.info("Successfully imported KafkaProducer after reinstall")
        except ImportError as e:
            logger.warning(f"Kafka Python client not available. Installing... Error: {str(e)}")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python==2.0.2"])
            from kafka.producer import KafkaProducer

        # Collect data to driver - this is safe as we're only working with batches
        event_messages = events_json_df.collect()
        total_events = len(event_messages)
        batch_size = 500

        logger.info(f"Processing {total_events} events in batches of {batch_size}")

        # Process in batches
        total_sent = 0
        for i in range(0, total_events, batch_size):
            # Calculate end index without using min()
            end_idx = i + batch_size
            if end_idx > total_events:
                end_idx = total_events

            batch = event_messages[i:end_idx]
            batch_size_actual = len(batch)

            # Calculate batch numbers without using min()
            total_batches = (total_events + batch_size - 1) // batch_size
            current_batch = i // batch_size + 1

            logger.info(
                f"Processing batch {current_batch} of {total_batches} ({batch_size_actual} events)")

            # Send this batch using Kafka Python client
            # Extract credentials from JAAS config string
            jaas_config = kafka_config["sasl.jaas.config"]
            username = "admin"  # Default
            password = None

            # Extract password from JAAS config
            if "password=" in jaas_config:
                try:
                    password = jaas_config.split('password="')[1].split('";')[0]
                except Exception as e:
                    logger.error(f"Error parsing JAAS config: {str(e)}")
                    password = None

            if not password:
                logger.error("Failed to extract password from JAAS config")
                return 0

            # Create producer with detailed logging
            logger.info(f"Creating Kafka producer for {kafka_config['bootstrap.servers']}")

            producer = None
            try:
                producer = KafkaProducer(
                    bootstrap_servers=kafka_config["bootstrap.servers"].split(','),
                    security_protocol=kafka_config["security.protocol"],
                    sasl_mechanism=kafka_config["sasl.mechanism"],
                    sasl_plain_username=username,
                    sasl_plain_password=password
                )
                logger.info("Kafka producer created successfully")

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
                logger.info(f"Producer flushed successfully")

                total_sent += batch_sent

                logger.info(f"Batch {current_batch} complete: Sent {batch_sent} events, {batch_errors} errors")

            except Exception as e:
                logger.error(f"Error creating or using Kafka producer: {str(e)}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                return total_sent
            finally:
                # Close the producer
                if producer:
                    producer.close()
                    logger.info("Kafka producer closed")

        logger.info(f"Successfully sent {total_sent} events to Kafka topic {kafka_config['topic']}")
        return total_sent
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
        raise
    finally:
        if 'spark' in locals() and spark:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except:
                pass


if __name__ == "__main__":
    main()