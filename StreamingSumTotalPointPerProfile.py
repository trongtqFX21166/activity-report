from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import CollectionInvalid, BulkWriteError
import os
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
        .config("spark.mongodb.input.uri", "mongodb://192.168.10.97:27017/activity_membershiptransactionmonthly_dev") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.10.97:27017/activity_membershiptransactionmonthly_dev") \
        .enableHiveSupport() \
        .getOrCreate()


def get_mongodb_client():
    """Create and return MongoDB client"""
    return MongoClient('mongodb://192.168.10.97:27017/')


def ensure_collection_exists(db, collection_name):
    """
    Ensure that the collection exists and has the proper indexes

    Args:
        db: MongoDB database connection
        collection_name: Name of the collection to ensure

    Returns:
        The collection object
    """
    # Check if collection exists in the database
    if collection_name not in db.list_collection_names():
        print(f"Collection {collection_name} does not exist. Creating it now.")
        # Create the collection explicitly
        db.create_collection(collection_name)

    # Get the collection
    collection = db[collection_name]

    # Create phone index if it doesn't exist
    existing_indexes = [idx['name'] for idx in collection.list_indexes()]

    if 'phone_1_month_1_year_1' not in existing_indexes:
        print(f"Creating compound index on phone, month, year for collection {collection_name}")
        collection.create_index([
            ("phone", ASCENDING),
            ("month", ASCENDING),
            ("year", ASCENDING)
        ], unique=True)

    # Additional useful indexes
    if 'totalpoints_-1' not in existing_indexes:
        print(f"Creating index on totalpoints for collection {collection_name}")
        collection.create_index([("totalpoints", -1)])

    if 'rank_1' not in existing_indexes:
        print(f"Creating index on rank for collection {collection_name}")
        collection.create_index([("rank", ASCENDING)])

    return collection


def process_monthly_points(batch_df, batch_id):
    """Process each batch and update monthly points in MongoDB"""
    if batch_df.isEmpty():
        return

    try:
        # Filter for AddPoint transactions only
        points_df = batch_df.filter(col("name") == "AddPoint")

        # Calculate monthly points per profile
        monthly_updates = points_df \
            .groupBy("phone", "membershipcode", "month", "year") \
            .agg(
            sum("value").alias("points"),
            max("timestamp").alias("timestamp")
        ).collect()

        if not monthly_updates:
            print(f"No points to process in batch {batch_id}")
            return

        # Initialize MongoDB client
        client = get_mongodb_client()
        db = client['activity_membershiptransactionmonthly_dev']

        # Group updates by collection (year_month)
        updates_by_collection = {}
        for row in monthly_updates:
            collection_name = f"{row['year']}_{row['month']}"
            if collection_name not in updates_by_collection:
                updates_by_collection[collection_name] = []

            # Create update operation
            update_op = UpdateOne(
                {
                    "phone": row['phone'],
                    "month": row['month'],
                    "year": row['year']
                },
                {
                    "$inc": {"totalpoints": row['points']},
                    "$set": {
                        "timestamp": row['timestamp'],
                        "last_updated": datetime.now()
                    },
                    "$setOnInsert": {
                        "rank": 9999,
                        "membershipcode": row['membershipcode'],
                        "membershipname": "Nhập môn",  # Default value
                        "created_at": datetime.now()
                    }
                },
                upsert=True
            )
            updates_by_collection[collection_name].append(update_op)

        # Execute updates for each collection
        for collection_name, operations in updates_by_collection.items():
            try:
                # Ensure collection exists with proper indexes
                collection = ensure_collection_exists(db, collection_name)

                # Execute bulk write operation
                result = collection.bulk_write(operations, ordered=False)
                print(f"Collection {collection_name} - Matched: {result.matched_count}, "
                      f"Modified: {result.modified_count}, Upserted: {result.upserted_count}")

            except BulkWriteError as bwe:
                # Handle duplicate key errors gracefully
                print(f"Encountered bulk write error for collection {collection_name}")
                print(f"Error details: {bwe.details}")

                # Process the writes that succeeded
                if 'nInserted' in bwe.details:
                    print(f"Inserted: {bwe.details['nInserted']}")
                if 'nMatched' in bwe.details:
                    print(f"Matched: {bwe.details['nMatched']}")
                if 'nModified' in bwe.details:
                    print(f"Modified: {bwe.details['nModified']}")
                if 'nUpserted' in bwe.details:
                    print(f"Upserted: {bwe.details['nUpserted']}")

                # Process one by one for retrying with error handling
                for op in operations:
                    try:
                        collection.update_one(
                            op._filter,
                            op._doc,
                            upsert=op._upsert
                        )
                    except Exception as e:
                        print(f"Error updating individual document: {str(e)}")
            except Exception as e:
                print(f"Error processing collection {collection_name}: {str(e)}")

        print(f"Successfully processed batch {batch_id}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise


def main():
    print("Starting monthly point updates processing")

    env = "dev"
    kafka_host_prod = "192.168.10.221:9093,192.168.10.222:9093,192.168.10.223:9093,192.168.10.171:9093"
    kafka_host_dev = "192.168.8.184:9092"

    # Initialize Spark Session
    spark = create_spark_session("monthly point updater")

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
        "kafka.group.id": "monthly-membership-point-updater-mongodb",
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
        "Name": "Unknown",
        "MembershipCode": "Level1"
    })

    # Convert column names to lowercase
    for column in trans_stream_df.columns:
        trans_stream_df = trans_stream_df.withColumnRenamed(column, column.lower())

    # Start streaming process
    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_monthly_points) \
        .option("checkpointLocation", "/activity_dev/chk-point-dir/monthly-point-updater-mongodb") \
        .trigger(processingTime='1 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()