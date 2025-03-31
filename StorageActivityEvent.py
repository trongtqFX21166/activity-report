from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = f'--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell'

# Configuration
env = "dev"
kafkaHostProd = "192.168.10.221:9093,192.168.10.222:9093,192.168.10.223:9093,192.168.10.171:9093"
kafkaHostDev = "192.168.8.184:9092"


def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()


def savingData(batch_df, batch_id):
    """Process each batch with deduplication before saving to Delta"""
    if batch_df.isEmpty():
        return

    try:
        spark = batch_df.sparkSession

        # Deduplicate by Id before processing
        deduplicated_df = batch_df.dropDuplicates(["Id"])

        # Log the deduplication
        original_count = batch_df.count()
        dedup_count = deduplicated_df.count()
        if original_count != dedup_count:
            print(f"Removed {original_count - dedup_count} duplicate events")

        # Use merge operation to prevent duplicates in Delta table
        try:
            # Get the Delta table
            delta_table = DeltaTable.forName(spark, "activity_dev.activity_event")

            # Perform merge operation to ensure idempotent processing
            delta_table.alias("target").merge(
                deduplicated_df.alias("source"),
                "target.Id = source.Id"
            ).whenNotMatchedInsertAll().execute()

            print(f"Merged {dedup_count} events into Delta table using merge")
        except Exception as e:
            print(f"Error during merge operation: {str(e)}")
            print("Falling back to append mode")

            # Fallback to append mode if merge fails
            deduplicated_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("Year", "Month", "Date", "Phone") \
                .saveAsTable("activity_dev.activity_event")

            print(f"Appended {dedup_count} events to Delta table")

        print(f"Successfully processed batch {batch_id}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise


if __name__ == '__main__':
    print("Starting activity event storage process")

    spark = create_spark_session("activity event storage")

    # Define schema for incoming events based on ActivityEventMsg
    schema = ArrayType(StructType([
        StructField("Id", StringType()),
        StructField("Phone", StringType()),
        StructField("Date", LongType()),
        StructField("Month", IntegerType()),
        StructField("Year", IntegerType()),
        StructField("TimeStamp", LongType()),
        StructField("MembershipCode", StringType()),
        StructField("Event", StringType()),
        StructField("ReportCode", StringType()),
        StructField("EventName", StringType()),
        StructField("ReportName", StringType()),
        StructField("Data", StringType())
    ]))

    # Configure Kafka stream
    kafka_options = {
        "kafka.bootstrap.servers": kafkaHostProd if env == "prod" else kafkaHostDev,
        "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"Vietmap2021!@#\";",
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "subscribe": "VML_ActivityEvent",
        "startingOffsets": "latest",  # Changed from "earliest" to "latest" to avoid reprocessing
        "kafka.group.id": "activity-spark-storage-activity-event",
        "inferSchema": "true",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000"
    }

    # Read from Kafka
    event_stream_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Parse and transform data
    event_stream_df = event_stream_df.select(
        explode(from_json(col("value").cast("string"), schema)).alias("value"),
        col("key").cast("string").alias("key")
    )

    event_stream_df = event_stream_df.select("value.*")

    # Replace null values and handle fields
    event_stream_df = event_stream_df.filter(
        col("Phone").isNotNull() &
        col("Event").isNotNull() &
        col("MembershipCode").isNotNull() &
        col("ReportCode").isNotNull()
    )

    event_stream_df = (event_stream_df
                       .fillna({"Data": "{}"})
                       )

    # Print schema for debugging
    event_stream_df.printSchema()

    # Start streaming with improved checkpoint location
    checkpoint_path = "/activity_dev/chk-point-dir/activity-event-storage-2"

    event_stream = event_stream_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(savingData) \
        .start()

    # Wait for termination
    spark.streams.awaitAnyTermination()