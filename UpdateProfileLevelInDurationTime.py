from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import uuid
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ProfileLevelUpgrader")

# Time window for profile updates (in minutes)
PROFILE_UPDATE_WINDOW_MINUTES = 30


# Calculate level thresholds dynamically
def calculate_level_thresholds(max_level=100):
    """Calculate thresholds for all levels using the formula:
    Level N threshold = Level (N-1) threshold + 150*(N-2)
    """
    thresholds = {
        1: 0,  # Level 1: >= 0 points
        2: 50,  # Level 2: >= 50 points
    }

    # Calculate Level 3 and beyond
    for level in range(3, max_level + 1):
        thresholds[level] = thresholds[level - 1] + 150 * (level - 2)

    return thresholds


# Define level thresholds (calculated up to level 100, but can be extended)
LEVEL_THRESHOLDS = calculate_level_thresholds(100)


# Environment functions
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


def get_postgres_config(env):
    """Return PostgreSQL configuration for the specified environment"""
    if env == 'dev':
        return {
            "host": "192.168.8.230",
            "database": "TrongTestDB1",
            "user": "postgres",
            "password": "admin123."
        }
    else:  # prod
        return {
            "host": "192.168.11.83",
            "database": "ActivityDB",
            "user": "vmladmin",
            "password": "5d6v6hiFGGns4onnZGW0VfKe"
        }


def get_kafka_config(env):
    """Return Kafka configuration for the specified environment"""
    if env == 'dev':
        return {
            "bootstrap.servers": "192.168.8.184:9092",
            "sasl.mechanism": "PLAIN",
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";'
        }
    else:  # prod
        return {
            "bootstrap.servers": "192.168.11.201:9092,192.168.11.202:9092,192.168.11.203:9092",
            "sasl.mechanism": "PLAIN",
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="3z740GCxK5xWfqoqKwxj";'
        }


def get_kafka_topic(env):
    """Return appropriate Kafka topic based on environment"""
    if env == 'dev':
        return "VML_ActivityTransaction"
    else:  # prod
        return "VML_ActivityTransaction"  # Adjust if different in production


# Print the first few levels and their thresholds for logging
logger.info("Level threshold calculation:")
for level in range(1, 11):
    logger.info(f"Level {level}: {LEVEL_THRESHOLDS[level]} points")


def create_spark_session(env):
    """Create a Spark session"""
    app_name = f"ProfileLevelUpgrader-{env}"

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_postgres_connection(env):
    """Create a connection to PostgreSQL database"""
    try:
        config = get_postgres_config(env)
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise


def get_current_datetime_epoch():
    """Get current date and time information in seconds since epoch"""
    now = datetime.now()
    timestamp = int(time.time())
    date_timestamp = int(datetime(now.year, now.month, now.day).timestamp())

    return {
        "timestamp": timestamp,
        "date_timestamp": date_timestamp,
        "month": now.month,
        "year": now.year,
        "datetime": now
    }


def fetch_recently_modified_profiles(conn, minutes=PROFILE_UPDATE_WINDOW_MINUTES):
    """Fetch profiles that have been modified within the specified time window"""
    try:
        # Calculate the cutoff time
        cutoff_time = datetime.now() - timedelta(minutes=minutes)

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                'SELECT "Id", "Phone", "TotalPoints", "Level", "MembershipCode", "LastModified" '
                'FROM public."Profiles" '
                'WHERE "LastModified" >= %s OR "LastModified" IS NULL',
                (cutoff_time,)
            )
            profiles = cursor.fetchall()
            logger.info(f"Fetched {len(profiles)} recently modified profiles (last {minutes} minutes)")
            return profiles
    except Exception as e:
        logger.error(f"Error fetching profiles: {str(e)}")
        raise


def determine_level(total_points):
    """Determine the correct level based on total points"""
    level = 1  # Default level

    for l, threshold in sorted(LEVEL_THRESHOLDS.items(), key=lambda x: x[0], reverse=True):
        if total_points >= threshold:
            level = l
            break

    return level


def create_level_upgrade_message(profile, new_level, dt_info):
    """Create a message for level upgrade"""
    message = {
        "Id": str(uuid.uuid4()),
        "Phone": profile["Phone"],
        "Date": dt_info["date_timestamp"],
        "Month": dt_info["month"],
        "Year": dt_info["year"],
        "ReportCode": "",
        "CampaignId": "8aaaa253-195f-4710-8d8f-23ce1e8c6f2d",
        "RuleId": "8aaaa253-195f-4710-8d8f-23ce1e8c6f2d",
        "CampaignName": "Cập nhật level",
        "RuleName": "Upgrade level profile rule",
        "EventCode": "UpgradeLevel",
        "EventName": "Nâng hạng cho người dùng",
        "ReportName": "Nâng hạng cho người dùng",
        "Name": "UpgradeLevel",
        "Value": new_level,
        "TimeStamp": dt_info["timestamp"],
        "MembershipCode": profile["MembershipCode"],
        "Type": "System"
    }

    return message


def update_profile_level(conn, profile_id, new_level):
    """Update the profile level in the database"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                'UPDATE public."Profiles" SET "Level" = %s, '
                '"LastModified" = %s, "LastModifiedBy" = %s '
                'WHERE "Id" = %s',
                (new_level, datetime.now(), "ProfileLevelUpgrader", profile_id)
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating profile {profile_id}: {str(e)}")
        return False


def send_kafka_messages(spark, messages, env):
    """Send messages to Kafka using Spark's Kafka producer"""
    if not messages:
        logger.info("No messages to send to Kafka")
        return

    # Get Kafka configuration for the environment
    kafka_config = get_kafka_config(env)
    kafka_topic = get_kafka_topic(env)

    logger.info(f"Sending {len(messages)} messages to Kafka topic {kafka_topic} in {env} environment")

    # Convert messages to DataFrame
    messages_schema = StructType([
        StructField("value", StringType(), True)
    ])

    # Convert messages to JSON strings
    json_messages = [json.dumps(msg) for msg in messages]
    messages_data = [(msg,) for msg in json_messages]

    # Create DataFrame with messages
    messages_df = spark.createDataFrame(messages_data, messages_schema)

    # Send to Kafka
    messages_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap.servers"]) \
        .option("topic", kafka_topic) \
        .option("kafka.sasl.mechanism", kafka_config["sasl.mechanism"]) \
        .option("kafka.security.protocol", kafka_config["security.protocol"]) \
        .option("kafka.sasl.jaas.config", kafka_config["sasl.jaas.config"]) \
        .save()

    logger.info(f"Successfully sent {len(messages)} messages to Kafka")


def process_profiles(env):
    """Main function to process all profiles"""
    conn = None
    spark = None

    try:
        # Setup connections
        conn = get_postgres_connection(env)
        spark = create_spark_session(env)

        # Get current date/time info
        dt_info = get_current_datetime_epoch()

        # Fetch recently modified profiles
        profiles = fetch_recently_modified_profiles(conn, PROFILE_UPDATE_WINDOW_MINUTES)

        if not profiles:
            logger.info(f"No profiles have been modified in the last {PROFILE_UPDATE_WINDOW_MINUTES} minutes. Exiting.")
            return

        # Process each profile
        total_processed = 0
        total_updated = 0
        kafka_messages = []

        for profile in profiles:
            try:
                total_points = profile["TotalPoints"] or 0  # Use 0 if None
                current_level = profile["Level"] or 1  # Use 1 if None

                # Determine the correct level based on points
                new_level = determine_level(total_points)

                # Check if level has changed
                if new_level != current_level:
                    logger.info(
                        f"Profile {profile['Phone']} needs level upgrade: {current_level} → {new_level} "
                        f"(Points: {total_points}, MembershipCode: {profile['MembershipCode']})"
                    )

                    # Update profile in database - only update the level, keep the existing membership code
                    if update_profile_level(conn, profile["Id"], new_level):
                        # Create Kafka message
                        message = create_level_upgrade_message(profile, new_level, dt_info)
                        kafka_messages.append(message)
                        total_updated += 1
            except Exception as e:
                logger.error(f"Error processing profile {profile.get('Phone', 'unknown')}: {str(e)}")
                continue

            total_processed += 1

            # Log progress periodically
            if total_processed % 100 == 0:
                logger.info(f"Processed {total_processed}/{len(profiles)} profiles")

        # Send kafka messages in batch
        if kafka_messages:
            send_kafka_messages(spark, kafka_messages, env)

        logger.info(f"Processing complete. Total profiles: {len(profiles)}, Updated: {total_updated}")

    except Exception as e:
        logger.error(f"Error in profile processing: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
        if spark:
            spark.stop()


def main():
    """Main entry point"""
    # Determine environment
    env = get_environment()
    logger.info(
        f"Starting Profile Level Upgrade Batch Process in {env.upper()} environment (window: {PROFILE_UPDATE_WINDOW_MINUTES} minutes)")

    try:
        # Process the profiles
        process_profiles(env)
        logger.info(f"Profile Level Upgrade Batch Process completed successfully in {env.upper()} environment")
    except Exception as e:
        logger.error(f"Profile Level Upgrade Batch Process failed in {env.upper()} environment: {str(e)}")
        raise


if __name__ == "__main__":
    main()