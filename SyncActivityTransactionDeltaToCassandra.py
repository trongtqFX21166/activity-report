from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import sys
from datetime import datetime, date


def create_spark_session():
    """Create Spark session with Cassandra configurations"""
    return SparkSession.builder \
        .appName("sync_activity_transaction_delta_to_cassandra") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.cassandra.connection.host", "192.168.8.165,192.168.8.166,192.168.8.183") \
        .enableHiveSupport() \
        .getOrCreate()


def read_cassandra_ids(spark, keyspace, table, year=None, month=None):
    """Read existing IDs from Cassandra table with year and month filtering"""
    try:
        # Start with the basic cassandra read
        cassandra_read = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", keyspace) \
            .option("table", table)

        # Apply filtering if year and month are provided
        query = None
        if year and month:
            query = f"year = {year} AND month = {month}"
            print(f"Filtering Cassandra records with: {query}")
        elif year:
            query = f"year = {year}"
            print(f"Filtering Cassandra records with: {query}")

        # Apply the filter if we have one
        if query:
            cassandra_read = cassandra_read.option("filter", query)

        # Complete the read and select only the ids
        existing_ids = cassandra_read.select("id").distinct()

        return existing_ids
    except Exception as e:
        print(f"Error reading Cassandra IDs: {str(e)}")
        # Return empty DataFrame if table doesn't exist yet
        return spark.createDataFrame([], "id string")


def convert_to_timestamp(year, month):
    """Convert year and month to timestamp (milliseconds since epoch)"""
    # Start of month
    start_of_month = int(datetime(year, month, 1).timestamp() * 1000)

    # End of month - find last day of month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    end_of_month = int((next_month - datetime.fromtimestamp(0.001)).total_seconds() * 1000) - 1

    return start_of_month, end_of_month


def process_transactions(year=None, month=None, batch_size=1000):
    """Process activity transactions and sync to Cassandra"""
    spark = None

    try:
        # Initialize Spark session
        spark = create_spark_session()

        # Set table and keyspace names
        keyspace = "activity_dev"
        table = "activitytransaction"

        print(f"Starting Delta to Cassandra sync at {datetime.now()}")

        # Read existing IDs from Cassandra for anti-join, filtering by year and month
        existing_ids_df = read_cassandra_ids(spark, keyspace, table, year, month)
        existing_count = existing_ids_df.count()
        print(f"Found {existing_count} existing records in Cassandra for the specified filters")

        # Read from Delta table
        delta_df = spark.read.format("delta").table("activity_dev.activity_transaction")

        # Apply year and month filters using Year and Month columns
        print(f"Filtering Delta table data...")
        if year and month:
            print(f"Filtering for Year={year}, Month={month}")
            delta_df = delta_df.filter((col("Year") == year) & (col("Month") == month))
        elif year:
            print(f"Filtering for Year={year}")
            delta_df = delta_df.filter(col("Year") == year)

        # Additional date range filter if needed
        if year and month:
            start_timestamp, end_timestamp = convert_to_timestamp(year, month)
            print(
                f"Date range: {datetime.fromtimestamp(start_timestamp / 1000)} to {datetime.fromtimestamp(end_timestamp / 1000)}")
            # We can use this for additional validation if needed, but primary filtering is done with Year/Month columns

        total_delta_records = delta_df.count()
        print(f"Found {total_delta_records} records in Delta table")

        # Find records that don't exist in Cassandra (anti-join)
        new_records = delta_df.join(existing_ids_df, "id", "left_anti")
        new_records_count = new_records.count()
        print(f"Found {new_records_count} new records to sync to Cassandra")

        if new_records_count == 0:
            print("No new records to sync. Exiting.")
            return

        # Convert column names to lowercase to match Cassandra schema
        lowercase_columns = []
        for column in new_records.columns:
            lowercase_columns.append(col(column).alias(column.lower()))

        new_records = new_records.select(*lowercase_columns)

        # Add processed timestamp
        new_records = new_records.withColumn("processed_timestamp", current_timestamp())

        # Process in batches to avoid memory issues
        if new_records_count > batch_size:
            print(f"Processing in batches of {batch_size} records")

            # Calculate number of partitions needed
            num_partitions = (new_records_count + batch_size - 1) // batch_size
            repartitioned_df = new_records.repartition(num_partitions)

            # Process each partition
            partitions = repartitioned_df.rdd.mapPartitions(lambda it: [list(it)]).collect()

            for i, partition in enumerate(partitions, 1):
                print(f"Processing batch {i}/{len(partitions)}")
                batch_df = spark.createDataFrame(partition, new_records.schema)

                # Write batch to Cassandra
                batch_df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("keyspace", keyspace) \
                    .option("table", table) \
                    .mode("append") \
                    .save()

                print(f"Completed batch {i}/{len(partitions)}")
        else:
            # Write all records in one go
            new_records.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", keyspace) \
                .option("table", table) \
                .mode("append") \
                .save()

        print(f"Successfully synced {new_records_count} records to Cassandra")

        # Show sample of synced records
        print("\nSample of synced records:")
        new_records.show(5, truncate=False)

    except Exception as e:
        print(f"Error syncing transactions: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


def main():
    """Main entry point with command line argument parsing"""

    # Parse command line arguments
    year = None
    month = None
    batch_size = 1000

    arg_count = len(sys.argv)
    if arg_count > 1:
        try:
            year = int(sys.argv[1])
            if arg_count > 2:
                month = int(sys.argv[2])
            if arg_count > 3:
                batch_size = int(sys.argv[3])
        except ValueError as e:
            print(f"Error parsing arguments: {str(e)}")
            print("Usage: script.py [year] [month] [batch_size]")
            print("Example: script.py 2024 12 1000")
            sys.exit(1)
    else:
        # If no args provided, use current year and month
        current_date = date.today()
        year = current_date.year
        month = current_date.month
        print(f"No date parameters provided. Using current year/month: {year}/{month}")

    # Validate inputs
    if month and (month < 1 or month > 12):
        print(f"Error: Month must be between 1 and 12, got {month}")
        sys.exit(1)

    if year and (year < 2000 or year > 2100):
        print(f"Warning: Year {year} seems unusual. Please verify this is correct.")

    # Run process with provided parameters
    process_transactions(year, month, batch_size)


if __name__ == "__main__":
    main()