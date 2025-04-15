from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 pyspark-shell'


def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.cassandra.connection.host", "192.168.8.165,192.168.8.166,192.168.8.183") \
        .enableHiveSupport() \
        .getOrCreate()


def save_to_storage(batch_df, batch_id):
    """Process each batch and save to both Cassandra and Delta Lake with deduplication"""
    if batch_df.isEmpty():
        return

    try:
        # Deduplicate by Id before processing
        deduplicated_df = batch_df.dropDuplicates(["Id"])

        # Log the deduplication
        original_count = batch_df.count()
        dedup_count = deduplicated_df.count()
        if original_count != dedup_count:
            print(f"Removed {original_count - dedup_count} duplicate transactions")

        # Save to Cassandra (using lowercase column names)
        cassandra_df = deduplicated_df.select([col(c).alias(c.lower()) for c in deduplicated_df.columns])

        cassandra_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="activitytransaction", keyspace="activity_dev") \
            .mode("append") \
            .save()

        # Delta Lake write with merge strategy to prevent duplicates
        spark = deduplicated_df.sparkSession

        # The table should already exist, so use merge operation directly
        try:
            # Get the Delta table
            delta_table = DeltaTable.forName(spark, "activity_dev.activity_transaction")

            # Perform merge operation to ensure idempotent processing
            delta_table.alias("target").merge(
                deduplicated_df.alias("source"),
                "target.Id = source.Id"
            ).whenNotMatchedInsertAll().execute()

            print(f"Merged {dedup_count} transactions into Delta table using merge")
        except Exception as e:
            print(f"Error during merge operation: {str(e)}")
            print("Falling back to append mode")

            # Fallback to append mode if merge fails
            deduplicated_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("Year", "Month", "Date", "Phone") \
                .saveAsTable("activity_dev.activity_transaction")

            print(f"Appended {dedup_count} transactions to Delta table")

        print(f"Successfully processed batch {batch_id}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise


def main():
    print("Starting activity transaction storage process")

    env = "dev"
    kafka_host_prod = "192.168.10.221:9093,192.168.10.222:9093,192.168.10.223:9093,192.168.10.171:9093"
    kafka_host_dev = "192.168.8.184:9092"

    # Initialize Spark Session
    spark = create_spark_session("activity transaction storage")

    # Define schema for the data
    schema = ArrayType(StructType([
        StructField("Id", StringType()),
        StructField("ActivityId", StringType()),
        StructField("Phone", StringType()),
        StructField("Date", LongType()),
        StructField("Month", IntegerType()),
        StructField("Year", IntegerType()),
        StructField("EventCode", StringType()),
        StructField("ReportCode", StringType()),
        StructField("CampaignId", StringType()),
        StructField("RuleId", StringType()),
        StructField("CampaignName", StringType()),
        StructField("RuleName", StringType()),
        StructField("EventName", StringType()),
        StructField("ReportName", StringType()),
        StructField("MembershipCode", StringType()),
        StructField("Name", StringType()),
        StructField("Value", IntegerType()),
        StructField("TimeStamp", LongType()),
        StructField("Type", StringType()),
    ]))

    # Initialize Kafka stream
    kafka_options = {
        "kafka.bootstrap.servers": kafka_host_prod if env == "prod" else kafka_host_dev,
        "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "subscribe": "VML_ActivityTransaction",
        "startingOffsets": "latest",  # Changed from "earliest" to "latest" to avoid reprocessing old messages
        "kafka.group.id": "activity-spark-storage-transaction",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000"
    }

    # Read from Kafka
    trans_stream_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Parse and transform data
    trans_stream_df = trans_stream_df.select(
        explode(from_json(col("value").cast("string"), schema)).alias("value"),
        col("key").cast("string").alias("key")
    )

    trans_stream_df = trans_stream_df.select("value.*")

    # If 'Type' is not in the schema, add it
    if "Type" not in trans_stream_df.columns:
        trans_stream_df = trans_stream_df.withColumn("Type", lit("Realtime"))

    # Handle null values
    trans_stream_df = trans_stream_df.filter(
        col("Phone").isNotNull() &
        col("EventCode").isNotNull() &
        col("MembershipCode").isNotNull()
    )

    # Start streaming process with improved checkpoint location
    checkpoint_path = "/activity_dev/chk-point-dir/activity-transaction-storage"

    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(save_to_storage) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='30 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == '__main__':
    main()