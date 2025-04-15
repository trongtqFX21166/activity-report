from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import os
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import sys

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.6.0 pyspark-shell'


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
    """Create and configure Spark session with environment-specific settings"""
    builder = SparkSession.builder \
        .appName(f"{app_name}-{env}") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return builder.enableHiveSupport().getOrCreate()


def get_postgres_connection(env):
    """Create a connection to the PostgreSQL database based on environment"""
    config = get_postgres_config(env)
    return psycopg2.connect(**config)


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


def process_point_updates(batch_df, batch_id, env):
    """Process each batch and update profile points in PostgreSQL with environment context"""
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
        conn = get_postgres_connection(env)
        try:
            update_profiles_batch(conn, updates)
            print(f"[{env.upper()}] Successfully updated {len(updates)} profiles in PostgreSQL")
        finally:
            conn.close()

        # Show sample results
        print(f"\n[{env.upper()}] Sample Updates:")
        profile_updates.show(5, truncate=False)

    except Exception as e:
        print(f"Error processing batch {batch_id} in {env} environment: {str(e)}")
        raise


def main():
    # Determine environment
    env = get_environment()
    print(f"Starting Profile Points Update Streaming Process in {env.upper()} environment")

    # Get Kafka configuration for the specified environment
    kafka_config = get_kafka_config(env)

    # Initialize Spark Session
    spark = create_spark_session("profile points updater", env)

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
        "kafka.bootstrap.servers": kafka_config["bootstrap.servers"],
        "kafka.sasl.jaas.config": kafka_config["sasl.jaas.config"],
        "kafka.security.protocol": kafka_config["security.protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl.mechanism"],
        "subscribe": "VML_ActivityTransaction",
        "startingOffsets": "earliest",
        "kafka.group.id": f"profile-points-updater-{env}",
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

    # Customize process_batch function to include environment
    def process_batch_with_env(batch_df, batch_id):
        return process_point_updates(batch_df, batch_id, env)

    # Environment-specific checkpoint location
    checkpoint_location = f"/activity_{env}/chk-point-dir/profile-points-updater"

    # Start streaming process
    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch_with_env) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='30 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == '__main__':
    main()