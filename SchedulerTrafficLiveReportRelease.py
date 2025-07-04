import psycopg2
import json
import time
import sys
import os
import uuid
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_json, struct, expr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SchedulerTrafficLiveReportRelease")

POSTGRES_CONFIG = {
    "dev": {
        "host": "192.168.8.47",  # Development host
        "database": "TrafficReportDb",
        "user": "postgres",
        "password": "admin123."
    },
    "prod": {
        "host": "192.168.11.187",  # Production host
        "database": "TrafficReportDb",
        "user": "postgres",
        "password": "X4WI6qRwlvHmexBteM1A"
    }
}

KAFKA_CONFIG = {
    "dev": {
        "bootstrap_servers": "192.168.8.184:9092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "admin",
        "sasl_password": "Vietmap2021!@#"
    },
    "prod": {
        "bootstrap_servers": "192.168.11.201:9092,192.168.11.202:9092,192.168.11.203:9092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "admin",
        "sasl_password": "3z740GCxK5xWfqoqKwxj"
    }
}


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


def connect_to_postgresql(env='dev'):
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG[env])
        return conn
    except Exception as e:
        logger.error(f"Error when connecting to PostgreSQL: {e}")
        return None


def get_live_reports(conn):
    try:
        cursor = conn.cursor()
        current_time = int(time.time())  # current Unix timestamp

        query = """
        SELECT id, cell_id, lat, lng, user_heading, category_id, expired_display_time, road_link_id,number_of_view, number_of_confirm, number_of_rejection, minimization_hex_color, geom_type, lines 
        FROM "live-report"
        WHERE expired_display_time > %s and status = 'Verified'
        """

        cursor.execute(query, (current_time,))
        results = cursor.fetchall()

        live_reports = []
        for row in results:
            report = {
                "id": str(row[0]),  # Convert UUID to string
                "cell_id": row[1],
                "lat": float(row[2]),  # Ensure proper type conversion
                "lng": float(row[3]),
                "user_heading": row[4],
                "category_id": str(row[5]),  # Convert UUID to string
                "expired_display_time": row[6],
                "number_of_view": row[7],
                "number_of_confirm": row[8],
                "number_of_rejection": row[9],
                "minimization_hex_color": row[10],
                "geom_type": row[11],
                "lines": row[12]
            }
            live_reports.append(report)

        cursor.close()
        return live_reports
    except Exception as e:
        logger.error(f"Error when querying data: {e}")
        return []


def create_spark_session(env='dev'):
    """Create Spark session with Kafka configurations"""
    kafka_config = KAFKA_CONFIG[env]

    # Configure SASL properties
    sasl_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["sasl_username"]}" '
        f'password="{kafka_config["sasl_password"]}";'
    )

    return SparkSession.builder \
        .appName("TrafficReport_LiveReport_ReleaseData") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .config("spark.kafka.sasl.jaas.config", sasl_config) \
        .config("spark.kafka.security.protocol", kafka_config["security_protocol"]) \
        .config("spark.kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .getOrCreate()


def format_data_for_kafka(data, trans_id, total_records):
    """Format data according to the new GeoJSON FeatureCollection structure"""
    try:
        # Create features array in GeoJSON format
        features = []
        for report in data:
            feature = {
                "geometry": {
                    "coordinates": [report["Lng"], report["Lat"]],  # [longitude, latitude]
                    "type": "Point"
                },
                "properties": {
                    "id": report["id"],
                    "heading": report["user_heading"],
                    "expiredDisplayTimeUnix": report["expired_display_time"],
                    "cellId": report["cell_id"] if report["cell_id"] else "",
                    "numberOfView": report["number_of_view"],
                    "numberOfConfirm": report["number_of_confirm"],
                    "numberOfRejection": report["number_of_rejection"],
                    "minimizationHexColor": report["minimization_hex_color"],
                },
                "type": "Feature"
            }
            features.append(feature)

        # Create the complete message structure
        message = {
            "trans_id": trans_id,
            "message_type": "LiveReportV2",
            "data": {
                "features": features,
                "type": "FeatureCollection"
            },
            "total_records": total_records
        }

        return message
    except Exception as e:
        logger.error(f"Error formatting data for Kafka: {e}")
        return None


def send_to_kafka_with_spark(spark, data, trans_id, totalRecords, env='dev'):
    """Send data to Kafka using Spark's Kafka integration"""
    try:
        if not data:
            logger.info("No data to send to Kafka")
            return

        from pyspark.sql.types import StructType, StructField, StringType

        kafka_config = KAFKA_CONFIG[env]
        topic = "MM_CompileLiveReport"
        bootstrap_servers = kafka_config["bootstrap_servers"]

        # Format data according to the new structure
        formatted_message = format_data_for_kafka(data, trans_id, totalRecords)
        if not formatted_message:
            logger.error("Failed to format data for Kafka")
            return

        # Convert message to JSON string
        message_json = json.dumps(formatted_message)

        # Create a single row DataFrame with the formatted message
        rows = [(message_json,)]
        schema = StructType([
            StructField("value", StringType(), False)
        ])

        message_df = spark.createDataFrame(rows, schema)

        # Send to Kafka
        logger.info(f"Sending {len(data)} reports to Kafka topic {topic}")
        logger.info(f"Sample message format: {json.dumps(formatted_message, indent=2)[:500]}...")

        # Configure Kafka writer with SASL properties
        message_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("kafka.security.protocol", kafka_config["security_protocol"]) \
            .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["sasl_username"]}" password="{kafka_config["sasl_password"]}";') \
            .option("topic", topic) \
            .mode("append") \
            .save()

        logger.info(f"Successfully sent data to Kafka topic {topic}")
    except Exception as e:
        logger.error(f"Error when sending data to Kafka: {e}")


def main():
    spark = None
    conn = None

    try:
        # Parse arguments
        import argparse
        parser = argparse.ArgumentParser(description='Scheduler Traffic Live Report Release')
        parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment to use (dev or prod)')
        parser.add_argument('--current-time', type=int, help='Current time as Unix timestamp (optional)')
        args = parser.parse_args()

        # Set environment based on arguments
        env = args.env.lower()
        logger.info(f"Starting job in {env} environment")

        # Initialize the database connection
        conn = connect_to_postgresql(env)
        if not conn:
            logger.error("Failed to connect to the database")
            return

        logger.info(f"Using database: {POSTGRES_CONFIG[env]['host']}/{POSTGRES_CONFIG[env]['database']}")

        # Initialize Spark session with Kafka configuration
        spark = create_spark_session(env)

        # Get live reports from the database
        live_reports = get_live_reports(conn)
        logger.info(f"Retrieved {len(live_reports)} live reports from database")

        # Define explicit schema for the message
        trans_id = str(uuid.uuid4())

        # Process and send data in batches
        if live_reports:
            batch_size = 100
            for i in range(0, len(live_reports), batch_size):
                batch = live_reports[i:i + batch_size]
                send_to_kafka_with_spark(spark, batch, trans_id, len(live_reports), env)
        else:
            logger.info("No live reports satisfying the condition")

        logger.info(f"Job completed successfully")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()