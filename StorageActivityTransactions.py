from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os
import sys

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 pyspark-shell'


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


def get_cassandra_config(env):
    """Return Cassandra configuration for the specified environment"""
    if env == 'dev':
        return {
            'contact_points': ["192.168.8.165", "192.168.8.166", "192.168.8.183"],
            'keyspace': 'activity_dev'
        }
    else:  # prod
        return {
            'contact_points': ["192.168.11.165", "192.168.11.166", "192.168.8.183"],
            'keyspace': 'activity'
        }


def get_delta_config(env):
    """Return Delta table configuration for the specified environment"""
    if env == 'dev':
        return {
            'table_name': 'activity_dev.activity_transaction',
            'storage_path': '/activity_dev/bronze_table/activity_transaction'
        }
    else:  # prod
        return {
            'table_name': 'activity.activity_transaction',
            'storage_path': '/activity/bronze_table/activity_transaction'
        }


def get_kafka_config(env):
    """Return Kafka configuration for the specified environment"""
    if env == 'dev':
        return {
            'bootstrap.servers': '192.168.8.184:9092',
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'group.id': 'activity-spark-storage-transaction'
        }
    else:  # prod
        return {
            'bootstrap.servers': '192.168.11.201:9092,192.168.11.202:9092,192.168.11.203:9092',
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="3z740GCxK5xWfqoqKwxj";',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'group.id': 'activity-spark-storage-transaction-prod'
        }


def create_spark_session(app_name, env):
    """Create Spark session with environment-specific configurations"""
    cassandra_config = get_cassandra_config(env)
    contact_points = ",".join(cassandra_config['contact_points'])

    return SparkSession.builder \
        .appName(f"{app_name}-{env}") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.cassandra.connection.host", contact_points) \
        .enableHiveSupport() \
        .getOrCreate()


def save_to_storage(batch_df, batch_id, env, delta_config, cassandra_config):
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

        print(f"[{env.upper()}] Saving {dedup_count} records to Cassandra keyspace {cassandra_config['keyspace']}")
        cassandra_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="activitytransaction", keyspace=cassandra_config['keyspace']) \
            .mode("append") \
            .save()

        # Delta Lake write with merge strategy to prevent duplicates
        spark = deduplicated_df.sparkSession
        table_name = delta_config['table_name']

        print(f"[{env.upper()}] Saving {dedup_count} records to Delta table {table_name}")

        # The table should already exist, so use merge operation directly
        try:
            # Get the Delta table
            delta_table = DeltaTable.forName(spark, table_name)

            # Perform merge operation to ensure idempotent processing
            delta_table.alias("target").merge(
                deduplicated_df.alias("source"),
                "target.Id = source.Id"
            ).whenNotMatchedInsertAll().execute()

            print(f"[{env.upper()}] Merged {dedup_count} transactions into Delta table using merge")
        except Exception as e:
            print(f"Error during merge operation: {str(e)}")
            print("Falling back to append mode")

            # Fallback to append mode if merge fails
            deduplicated_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("Year", "Month", "Date", "Phone") \
                .saveAsTable(table_name)

            print(f"[{env.upper()}] Appended {dedup_count} transactions to Delta table")

        print(f"[{env.upper()}] Successfully processed batch {batch_id}")

    except Exception as e:
        print(f"[{env.upper()}] Error processing batch {batch_id}: {str(e)}")
        raise


def main():
    # Determine environment
    env = get_environment()
    print(f"Starting activity transaction storage process in {env.upper()} environment")

    # Get environment-specific configurations
    kafka_config = get_kafka_config(env)
    cassandra_config = get_cassandra_config(env)
    delta_config = get_delta_config(env)

    # Initialize Spark Session
    spark = create_spark_session("activity transaction storage", env)

    # Define schema for the data
    schema = ArrayType(StructType([
        StructField("Id", StringType()),
        StructField("Phone", StringType()),
        StructField("Date", LongType()),
        StructField("Month", IntegerType()),
        StructField("Year", IntegerType()),
        StructField("EventCode", StringType()),
        StructField("ReportCode", StringType()),
        StructField("ActivityId", StringType()),
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
        "kafka.bootstrap.servers": kafka_config["bootstrap.servers"],
        "kafka.sasl.jaas.config": kafka_config["sasl.jaas.config"],
        "kafka.security.protocol": kafka_config["security.protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl.mechanism"],
        "subscribe": "VML_ActivityTransaction",
        "startingOffsets": "latest",  # Changed from "earliest" to "latest" to avoid reprocessing old messages
        "kafka.group.id": kafka_config["group.id"],
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

    # Customize save_to_storage function to include environment context
    def process_batch_with_env(batch_df, batch_id):
        return save_to_storage(batch_df, batch_id, env, delta_config, cassandra_config)

    # Environment-specific checkpoint location
    checkpoint_path = f"/activity_{env}/chk-point-dir/activity-transaction-storage"

    # Start streaming process
    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch_with_env) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='10 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == '__main__':
    main()