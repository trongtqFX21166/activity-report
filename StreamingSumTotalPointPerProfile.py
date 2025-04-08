from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import CollectionInvalid, BulkWriteError
import os
import sys
from datetime import datetime

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'


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


def get_mongodb_config(env):
    """Return MongoDB configuration for the specified environment"""
    if env == 'dev':
        return {
            'host': 'mongodb://192.168.10.97:27017',
            'database': 'activity_membershiptransactionmonthly_dev'
        }
    else:  # prod
        return {
            'host': 'mongodb://admin:gctStAiH22368l5qziUV@192.168.11.171:27017,192.168.11.172:27017,192.168.11.173:27017',
            'database': 'activity_membershiptransactionmonthly',
            'auth_source': 'admin'
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
    """Create Spark session with environment-specific MongoDB configurations"""
    mongo_config = get_mongodb_config(env)

    builder = SparkSession.builder \
        .appName(f"{app_name}-{env}") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Add MongoDB configuration
    builder = builder \
        .config("spark.mongodb.input.uri", f"{mongo_config['host']}/{mongo_config['database']}") \
        .config("spark.mongodb.output.uri", f"{mongo_config['host']}/{mongo_config['database']}")

    if 'auth_source' in mongo_config:
        builder = builder \
            .config("spark.mongodb.input.uri.authSource", mongo_config['auth_source']) \
            .config("spark.mongodb.output.uri.authSource", mongo_config['auth_source'])

    return builder.enableHiveSupport().getOrCreate()


def get_mongodb_client(env):
    """Create and return MongoDB client based on environment"""
    mongo_config = get_mongodb_config(env)

    # Create MongoDB client with appropriate settings
    if env == 'dev':
        return MongoClient(mongo_config['host'])
    else:  # prod
        return MongoClient(
            mongo_config['host'],
            authSource=mongo_config['auth_source']
        )


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


def process_monthly_points(batch_df, batch_id, env):
    """Process each batch and update monthly points in MongoDB"""
    if batch_df.isEmpty():
        return

    try:
        print(f"Processing batch {batch_id} in {env.upper()} environment")

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
        client = get_mongodb_client(env)
        mongo_config = get_mongodb_config(env)
        db = client[mongo_config['database']]

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
    finally:
        if 'client' in locals():
            client.close()


def main():
    # Determine environment
    env = get_environment()
    print(f"Starting monthly point updates processing in {env.upper()} environment")

    # Get Kafka configuration for the environment
    kafka_config = get_kafka_config(env)

    # Initialize Spark Session
    spark = create_spark_session("monthly point updater", env)

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
        "kafka.group.id": f"monthly-membership-point-updater-mongodb-{env}",
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

    # Customize process_batch function to include environment
    def process_batch_with_env(batch_df, batch_id):
        return process_monthly_points(batch_df, batch_id, env)

    # Environment-specific checkpoint location
    checkpoint_location = f"/activity_{env}/chk-point-dir/monthly-point-updater-mongodb"

    # Start streaming process
    streaming_query = trans_stream_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch_with_env) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='1 seconds') \
        .start()

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()