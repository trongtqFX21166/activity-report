from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pymongo import MongoClient
from pymongo.operations import ReplaceOne
from datetime import datetime
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'

# Initialize environment variables
env = "dev"
kafka_host_prod = "192.168.10.221:9093,192.168.10.222:9093,192.168.10.223:9093,192.168.10.171:9093"
kafka_host_dev = "192.168.8.184:9092"


def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.mongodb.input.uri", "mongodb://192.168.10.97:27017/activity_events_dev") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.10.97:27017/activity_events_dev") \
        .enableHiveSupport() \
        .getOrCreate()


def save_to_storage(batch_df, batch_id):
    """Process each batch and save to both Delta Lake and MongoDB"""
    if batch_df.isEmpty():
        return

    try:
        # Save to Delta Lake
        print(f"Saving batch {batch_id} to Delta Lake...")
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("Year", "Month", "Date", "Phone") \
            .saveAsTable("activity_dev.activity_event")

        # Convert DataFrame to list of dictionaries for MongoDB
        records = batch_df.collect()
        mongo_docs = []

        for record in records:
            # Convert to dictionary
            doc = record.asDict()

            # Format collection name based on year and month
            collection_name = f"{doc['Year']}_{doc['Month']}"

            # Add metadata
            doc['_id'] = doc['Id']  # Use the Id field as MongoDB _id
            doc['processed_timestamp'] = datetime.now().isoformat()

            mongo_docs.append((collection_name, doc))

        # Group documents by collection
        docs_by_collection = {}
        for collection_name, doc in mongo_docs:
            if collection_name not in docs_by_collection:
                docs_by_collection[collection_name] = []
            docs_by_collection[collection_name].append(doc)

        # Initialize MongoDB client
        client = MongoClient('mongodb://192.168.10.97:27017/')
        db = client['activity_events_dev']

        # Bulk replace documents in appropriate collections
        print(f"Saving batch {batch_id} to MongoDB...")
        for collection_name, docs in docs_by_collection.items():
            collection = db[collection_name]
            # Prepare bulk replace operations
            operations = [
                ReplaceOne(
                    {'_id': doc['_id']},
                    doc,
                    upsert=True
                ) for doc in docs
            ]
            # Execute bulk write
            result = collection.bulk_write(operations, ordered=False)
            print(
                f"MongoDB batch {batch_id} results - Inserted: {result.upserted_count}, Modified: {result.modified_count}, Matched: {result.matched_count}")

        print(f"Successfully processed batch {batch_id} - Saved {len(records)} documents to both storages")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise


def main():
    print("Starting activity event storage process")

    # Initialize Spark Session
    spark = create_spark_session("activity event storage")

    # Define schema for incoming events
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

    # Configure Kafka connection
    kafka_options = {
        "kafka.bootstrap.servers": kafka_host_prod if env == "prod" else kafka_host_dev,
        "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "subscribe": "VML_ActivityEvent",
        "startingOffsets": "earliest",
        "kafka.group.id": "activity-spark-storage-event-mongo",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000"
    }

    # Read from Kafka stream
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

    # Handle null values
    event_stream_df = event_stream_df \
        .fillna({"MembershipCode": "Unknown"}) \
        .fillna({"Event": "Other"}) \
        .fillna({"Data": "{}"}) \
        .fillna({"ReportCode": "Unknown"}) \
        .fillna({"Phone": "unknown"})

    # Print schema for debugging
    event_stream_df.printSchema()

    # Start streaming process
    streaming_query = event_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(save_to_storage) \
        .option("checkpointLocation", "/activity_dev/chk-point-dir/activity-event-storage-mongo") \
        .trigger(processingTime='1 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()