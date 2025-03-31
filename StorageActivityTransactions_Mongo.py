from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import os
from pymongo import MongoClient, UpdateOne
import json
from datetime import datetime

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'


def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.mongodb.input.uri", "mongodb://192.168.10.97:27017/activity_transactions_dev") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.10.97:27017/activity_transactions_dev") \
        .enableHiveSupport() \
        .getOrCreate()


def save_to_storage(batch_df, batch_id):
    """Process each batch and save to both MongoDB and Delta Lake"""
    if batch_df.isEmpty():
        return

    try:
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
        db = client['activity_transactions_dev']

        total_upserted = 0
        total_modified = 0

        # Process documents using bulk upsert operations
        for collection_name, docs in docs_by_collection.items():
            collection = db[collection_name]

            # Create bulk upsert operations
            bulk_operations = [
                UpdateOne(
                    {'_id': doc['_id']},  # Filter by _id
                    {'$set': doc},  # Update/insert full document
                    upsert=True  # Create if doesn't exist
                ) for doc in docs
            ]

            try:
                # Execute bulk operations
                result = collection.bulk_write(bulk_operations, ordered=False)
                total_upserted += result.upserted_count
                total_modified += result.modified_count

                print(f"Collection {collection_name} results - "
                      f"Upserted: {result.upserted_count}, "
                      f"Modified: {result.modified_count}, "
                      f"Matched: {result.matched_count}")

            except Exception as e:
                print(f"Error processing collection {collection_name}: {str(e)}")
                raise

        # Save to Delta Lake
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("Year", "Month", "Date", "Phone") \
            .saveAsTable("activity_dev.activity_transaction")

        print(f"Successfully processed batch {batch_id} - "
              f"Total upserted: {total_upserted}, "
              f"Total modified: {total_modified}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()


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
    ]))

    # Initialize Kafka stream
    kafka_options = {
        "kafka.bootstrap.servers": kafka_host_prod if env == "prod" else kafka_host_dev,
        "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Vietmap2021!@#";',
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "subscribe": "VML_ActivityTransaction",
        "startingOffsets": "earliest",
        "kafka.group.id": "activity-spark-storage-transaction-mongodb",
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

    trans_stream_df = trans_stream_df.select("value.*") \
        .withColumn("Type", lit("Other"))

    # Handle null values
    trans_stream_df = (trans_stream_df
                       .fillna({"MembershipCode": "Level1"})
                       .fillna({"EventCode": "Other"}))

    # Print schema for debugging
    trans_stream_df.printSchema()

    # Start streaming process
    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(save_to_storage) \
        .option("checkpointLocation", "/activity_dev/chk-point-dir/activity-transaction-storage-mongodb") \
        .trigger(processingTime='1 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == '__main__':
    main()