from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from redis import Redis
import json
import sys
from datetime import datetime


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("membership-yearly-redis-sync") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.write.connection.uri", "mongodb://192.168.10.97:27017") \
        .config("spark.mongodb.read.database", "activity_membershiptransactionyearly_dev") \
        .config("spark.mongodb.write.database", "activity_membershiptransactionyearly_dev") \
        .getOrCreate()


def setup_redis_connection():
    """Setup Redis connection"""
    try:
        redis_client = Redis(
            host='192.168.8.226',
            port=6379,
            password='0ef1sJm19w3OKHiH',
            decode_responses=True
        )
        redis_client.ping()
        print("Successfully connected to Redis")
        return redis_client
    except Exception as e:
        print(f"Failed to connect to Redis: {str(e)}")
        sys.exit(1)


def generate_document_id(year, phone):
    """Generate Redis document ID following the index pattern"""
    print(f"activity-dev:membership:yearly:{year}:{phone}")
    return f"activity-dev:membership:yearly:{year}:{phone}"


def process_batch(batch_df, redis_client):
    """Process a batch of records and upsert to Redis"""
    try:
        records = batch_df.collect()
        print(f"Processing {len(records)} records in batch")

        inserts = 0
        updates = 0
        errors = 0

        # Process in smaller sub-batches for better control
        sub_batch_size = 50
        for i in range(0, len(records), sub_batch_size):
            sub_batch = records[i:i + sub_batch_size]
            pipeline = redis_client.pipeline(transaction=False)  # Use non-transactional pipeline for better performance

            sub_batch_inserts = 0
            for record in sub_batch:
                try:
                    # Generate document ID
                    print(record)
                    doc_id = generate_document_id(record['year'], record['phone'])
                    print(f"Processing: {doc_id}")

                    # Create new document
                    new_doc = {
                        "Id": doc_id,
                        "Phone": record['phone'],
                        "MembershipCode": record['membershipcode'],
                        "Year": record['year'],
                        "Rank": record['rank'],
                        "MembershipName": record['membershipname'],
                        "TotalPoints": record['totalpoints']
                    }

                    # Queue delete and set operations
                    pipeline.delete(doc_id)
                    pipeline.json().set(doc_id, "$", new_doc)
                    pipeline.sadd("activity-ranking-yearly-idx", doc_id)
                    sub_batch_inserts += 1

                except Exception as e:
                    print(f"Error preparing record {record['phone']}: {str(e)}")
                    errors += 1
                    continue

            # Execute pipeline for sub-batch
            try:
                pipeline.execute()
                inserts += sub_batch_inserts
            except Exception as e:
                print(f"Error executing pipeline: {str(e)}")
                # If pipeline fails, try records individually
                for record in sub_batch:
                    try:
                        doc_id = generate_document_id(record['year'], record['phone'])
                        new_doc = {
                            "Id": doc_id,
                            "Phone": record['phone'],
                            "MembershipCode": record['membershipcode'],
                            "Year": record['year'],
                            "Rank": record['rank'],
                            "MembershipName": record['membershipname'],
                            "TotalPoints": record['totalpoints']
                        }

                        # Clean up and retry individual record
                        redis_client.delete(doc_id)
                        redis_client.json().set(doc_id, "$", new_doc)
                        redis_client.sadd("activity-ranking-yearly-idx", doc_id)
                        inserts += 1
                    except Exception as individual_error:
                        print(f"Error processing individual record {record['phone']}: {str(individual_error)}")
                        errors += 1

        print(f"Sub-batch processing details:")
        print(f"  Records to process: {len(records)}")
        print(f"  Successfully inserted: {inserts}")
        print(f"  Errors: {errors}")

        return inserts, updates, errors

    except Exception as e:
        print(f"Error processing batch: {str(e)}")
        raise


def read_from_mongodb(spark, current_year):
    """Read data from MongoDB with current month/year filter"""
    collection_name = f"{current_year}"
    print(f"Reading data from MongoDB collection: {collection_name}")

    return spark.read \
        .format("mongodb") \
        .option("collection", collection_name) \
        .option("readConcern.level", "majority") \
        .load()


def process_data(spark, redis_client):
    """Main data processing function"""
    try:
        # Get current month and year
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year

        print(f"Starting data processing for {current_year}...")

        # Read from MongoDB with current month/year filter
        df = read_from_mongodb(spark, current_year)
        total_records = df.count()
        df.show()
        print(f"Found {total_records} records to process")

        if total_records == 0:
            print(f"No records found for {current_year}")
            return

        # Configure batching
        batch_size = 200  # Smaller batch size for better control

        # Repartition the dataframe for more efficient processing
        num_partitions = (total_records + batch_size - 1) // batch_size
        df_partitioned = df.repartition(num_partitions)

        total_inserts = 0
        total_updates = 0
        total_errors = 0

        # Process each partition
        partitions = df_partitioned.rdd.mapPartitions(lambda it: [list(it)]).collect()

        for batch_num, partition in enumerate(partitions, 1):
            print(f"\nProcessing batch {batch_num}/{len(partitions)}")
            print(f"Batch size: {len(partition)} records")

            # Create a new DataFrame for the partition
            batch_df = spark.createDataFrame(partition, df.schema)

            # Convert column names to lowercase to match Redis document structure
            for column in batch_df.columns:
                batch_df = batch_df.withColumnRenamed(column, column.lower())

            inserts, updates, errors = process_batch(batch_df, redis_client)
            total_inserts += inserts
            total_updates += updates
            total_errors += errors

            print(f"Completed batch {batch_num}/{len(partitions)}")

        print(f"\nProcessing Summary:")
        print(f"Total records processed: {total_records}")
        print(f"Total inserts: {total_inserts}")
        print(f"Total updates: {total_updates}")
        print(f"Total errors: {total_errors}")

    except Exception as e:
        print(f"Error in data processing: {str(e)}")
        raise


def main():
    current_date = datetime.now()
    print(f"Starting Membership yearly Redis Sync Job at {current_date}")

    spark = None
    redis_client = None

    try:
        # Initialize Spark
        spark = create_spark_session()

        # Initialize Redis
        redis_client = setup_redis_connection()

        # Process data for current month/year
        process_data(spark, redis_client)

        print(f"Job completed successfully at {datetime.now()}")

    except Exception as e:
        print(f"Job failed: {str(e)}")
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
        if redis_client:
            redis_client.close()


if __name__ == "__main__":
    main()