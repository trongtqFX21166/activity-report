from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, date
import psycopg2
from psycopg2.extras import execute_batch
import os


def create_spark_session():
    return SparkSession.builder \
        .appName("activity_summary_to_postgres") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .getOrCreate()


def get_postgres_connection():
    return psycopg2.connect(
        host="192.168.8.230",
        database="TrongTestDB1",
        user="postgres",
        password="admin123."
    )


def get_current_date_timestamp():
    """Get start and end timestamps for current date"""
    today = date.today()
    # Convert to timestamp (seconds since epoch)
    start_of_day = int(datetime(today.year, today.month, today.day).timestamp() * 1000)
    end_of_day = int(datetime(today.year, today.month, today.day, 23, 59, 59).timestamp() * 1000)
    return start_of_day, end_of_day


def insert_summaries_batch(conn, summaries):
    insert_query = """
        INSERT INTO public."ActivitySummaryReports"
        ("Phone", "Date", "TotalSubmittedReport", "TotalApprovedReport", 
         "TotalDrivingReport", "CreatedAt", "CreatedBy", 
         "LastModified", "LastModifiedBy")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    current_time = datetime.now()
    created_by = "ActivitySummaryProcessor"

    # Prepare data for batch insert
    batch_data = [
        (
            row['phone'],
            row['date'],
            row['total_submitted_report'],
            row['total_approved_report'],
            row['total_driving_report'],
            current_time,
            created_by,
            current_time,
            created_by
        )
        for row in summaries
    ]

    with conn.cursor() as cur:
        execute_batch(cur, insert_query, batch_data, page_size=1000)
    conn.commit()


def process_activity_summary():
    try:
        spark = create_spark_session()

        # Get current date timestamps
        start_timestamp, end_timestamp = get_current_date_timestamp()

        print(
            f"Processing activities for date range: {datetime.fromtimestamp(start_timestamp / 1000)} to {datetime.fromtimestamp(end_timestamp / 1000)}")

        # Read activity events with date filter
        events_df = spark.read.format("delta").table("activity_dev.activity_event") \
            .filter((col("timestamp") >= start_timestamp) & (col("timestamp") <= end_timestamp))

        # Log the count of records being processed
        event_count = events_df.count()
        print(f"Found {event_count} events for current date")

        if event_count == 0:
            print("No events found for current date. Exiting.")
            return

        # Calculate event type summaries
        summary_df = events_df.groupBy("phone", "date") \
            .agg(
            sum(when(col("event") == "SubmittedReport", 1).otherwise(0)).alias("total_submitted_report"),
            sum(when(col("event") == "ApprovedReport", 1).otherwise(0)).alias("total_approved_report"),
            sum(when(col("event") == "DrivingReport", 1).otherwise(0)).alias("total_driving_report")
        )

        # Convert to rows for processing
        summaries = summary_df.collect()

        # Convert to list of dictionaries for easier processing
        summary_data = [row.asDict() for row in summaries]

        # Check if we have any summaries to insert
        if not summary_data:
            print("No summaries generated for current date. Exiting.")
            return

        # Store in PostgreSQL
        conn = get_postgres_connection()
        try:
            insert_summaries_batch(conn, summary_data)
            print(f"Successfully inserted {len(summary_data)} summary records into PostgreSQL")
        finally:
            conn.close()

        # Show sample results
        print("\nSample Results:")
        summary_df.show(5, truncate=False)

    except Exception as e:
        print(f"Error processing activity summary: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


def main():
    try:
        # Initialize logging
        print(f"Starting Activity Summary Processing for date: {date.today()}")

        # Process summaries
        process_activity_summary()

        print("Activity Summary Processing Completed Successfully!")

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        raise


if __name__ == "__main__":
    main()