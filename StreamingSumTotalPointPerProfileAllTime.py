from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import os
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.6.0 pyspark-shell'


def create_spark_session(app_name):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()


def get_postgres_connection():
    """Create a connection to the PostgreSQL database"""
    return psycopg2.connect(
        host="192.168.8.230",  # This is the host from ActivityEventSummaryReport.py
        database="TrongTestDB1",  # Using the same database as in the sample code
        user="postgres",
        password="admin123."
    )


def update_profiles_batch(conn, updates):
    """Update profile points in batch"""
    update_query = """
        UPDATE public."Profiles"
        SET "TotalPoints" = "TotalPoints" + %s,
            "LastModified" = %s,
            "LastModifiedBy" = %s
        WHERE "Phone" = %s
    """

    current_time = datetime.now()
    modified_by = "StreamingPointUpdater"

    # Prepare data for batch update
    batch_data = [
        (
            row['total_points'],
            current_time,
            modified_by,
            row['phone']
        )
        for row in updates
    ]

    with conn.cursor() as cur:
        execute_batch(cur, update_query, batch_data, page_size=1000)
    conn.commit()


def process_point_updates(batch_df, batch_id):
    """Process each batch and update profile points in PostgreSQL"""
    if batch_df.isEmpty():
        return

    try:
        # Filter for AddPoint transactions only
        points_df = batch_df.filter(col("name") == "AddPoint")

        if points_df.count() == 0:
            print(f"No point transactions in batch {batch_id}")
            return

        # Calculate points per profile
        profile_updates = points_df \
            .groupBy("phone") \
            .agg(sum("value").alias("total_points"))

        # Convert to list of dictionaries for easier processing
        updates = [row.asDict() for row in profile_updates.collect()]

        if not updates:
            print(f"No updates to process in batch {batch_id}")
            return

        # Store updates in PostgreSQL
        conn = get_postgres_connection()
        try:
            update_profiles_batch(conn, updates)
            print(f"Successfully updated {len(updates)} profiles in PostgreSQL")
        finally:
            conn.close()

        # Show sample results
        print("\nSample Updates:")
        profile_updates.show(5, truncate=False)

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise


def main():
    print("Starting Profile Points Update Streaming Process")

    env = "dev"
    kafka_host_prod = "192.168.10.221:9093,192.168.10.222:9093,192.168.10.223:9093,192.168.10.171:9093"
    kafka_host_dev = "192.168.8.184:9092"

    # Initialize Spark Session
    spark = create_spark_session("profile points updater")

    # Define schema based on VML_ActivityTransaction message
    schema = ArrayType(StructType([
        StructField("Id", StringType()),
        StructField("Phone", StringType()),
        StructField("Date", LongType()),
        StructField("Month", IntegerType()),
        StructField("Year", IntegerType()),
        StructField("ReportCode", StringType()),
        StructField("CampaignId", StringType()),
        StructField("RuleId", StringType()),
        StructField("CampaignName", StringType()),
        StructField("RuleName", StringType()),
        StructField("EventCode", StringType()),
        StructField("EventName", StringType()),
        StructField("ReportName", StringType()),
        StructField("Name", StringType()),
        StructField("Value", IntegerType()),
        StructField("TimeStamp", LongType()),
        StructField("MembershipCode", StringType()),
        StructField("Type", StringType())
    ]))

    # Configure Kafka connection
    kafka_options = {
        "kafka.bootstrap.servers": kafka_host_prod if env == "prod" else kafka_host_dev,
        "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "subscribe": "VML_ActivityTransaction",
        "startingOffsets": "earliest",
        "kafka.group.id": "profile-points-updater",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000"
    }

    # Read from Kafka stream
    trans_stream_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Parse JSON and transform data
    trans_stream_df = trans_stream_df.select(
        explode(from_json(col("value").cast("string"), schema)).alias("value"),
        col("key").cast("string").alias("key")
    )

    trans_stream_df = trans_stream_df.select("value.*")

    # Handle null values
    trans_stream_df = trans_stream_df.fillna({
        "Value": 0,
        "Name": "Unknown"
    })

    # Convert column names to lowercase for consistency
    for column in trans_stream_df.columns:
        trans_stream_df = trans_stream_df.withColumnRenamed(column, column.lower())

    # Start streaming process
    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_point_updates) \
        .option("checkpointLocation", "/activity_dev/chk-point-dir/profile-points-updater") \
        .trigger(processingTime='30 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == '__main__':
    main()