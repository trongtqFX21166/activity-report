from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import os
import logging
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LiveReportVerificationJob")

# PostgreSQL configuration - adjust as needed
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

# Default environment
ENV = "dev"  # Set to "prod" to use production database


def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("live_report_verification_job") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_postgres_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG[ENV])
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise


def update_report_statuses():
    """Update live reports from WaitingVerification to Verified"""
    conn = None

    try:
        # Get current timestamp for logging
        start_time = datetime.now()
        logger.info(f"Starting live report verification job at {start_time}")

        # Connect to PostgreSQL
        conn = get_postgres_connection()

        # Fetch reports that need verification - only those with status "WaitingVerification" and submitted over 5 minutes ago
        current_time = int(time.time())  # Current Unix timestamp
        five_minutes_ago = current_time - (5 * 60)  # 5 minutes in seconds

        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT "id" FROM public."live-report" WHERE "status" = %s AND "submit_time" <= %s',
                ('WaitingVerification', five_minutes_ago)
            )
            reports = cursor.fetchall()

        report_count = len(reports)
        logger.info(f"Found {report_count} reports to verify")

        if report_count == 0:
            logger.info("No reports need verification. Exiting.")
            return 0

        # Prepare update query
        update_query = """
            UPDATE public."live-report" 
            SET "status" = %s, 
                "last_modified" = %s, 
                "last_modified_by" = %s 
            WHERE "id" = %s
        """

        # Prepare batch data
        current_time = int(time.time())  # Unix timestamp matching the schema
        modified_by = "LiveReportVerificationJob"

        batch_data = []
        for row in reports:
            report_id = row[0]
            batch_data.append(
                (
                    "Verified",  # New status
                    current_time,  # last_modified
                    modified_by,  # last_modified_by
                    report_id  # id for WHERE clause
                )
            )

        # Execute batch update
        with conn.cursor() as cursor:
            execute_batch(cursor, update_query, batch_data, page_size=1000)

        # Commit the transaction
        conn.commit()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"Verification job completed at {end_time}")
        logger.info(f"Updated {report_count} reports from 'WaitingVerification' to 'Verified'")
        logger.info(f"Total duration: {duration:.2f} seconds")

        return report_count

    except Exception as e:
        logger.error(f"Error updating report statuses: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def main():
    """Main function"""
    spark = None

    try:
        # Parse command line arguments to determine environment
        import argparse
        parser = argparse.ArgumentParser(description='Live Report Verification Job')
        parser.add_argument('--env', choices=['dev', 'prod'], default='dev', help='Environment to use (dev or prod)')
        args = parser.parse_args()

        # Set environment based on arguments
        global ENV
        ENV = args.env.lower()

        logger.info(f"Starting job in {ENV} environment")
        logger.info(f"Using database: {POSTGRES_CONFIG[ENV]['host']}/{POSTGRES_CONFIG[ENV]['database']}")

        # Initialize Spark (may not be needed for this specific job but included for consistency)
        spark = create_spark_session()

        # Update report statuses
        updated_count = update_report_statuses()

        logger.info(f"Job completed successfully. Updated {updated_count} reports.")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()