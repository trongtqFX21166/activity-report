from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, max as pyspark_max
from pyspark.sql.window import Window
import os
import argparse
from datetime import datetime


def create_spark_session():
    """Create Spark session with Delta Lake configurations"""
    return SparkSession.builder \
        .appName("activity_deduplication") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .enableHiveSupport() \
        .getOrCreate()


def deduplicate_delta_table(spark, table_name, id_column="Id", timestamp_column="TimeStamp"):
    """
    Deduplicate a Delta table by keeping the record with the latest timestamp for each ID.

    Parameters:
    - spark: SparkSession
    - table_name: Name of the Delta table (e.g., "activity_dev.activity_transaction")
    - id_column: Name of the column containing the unique identifier
    - timestamp_column: Name of the column containing the timestamp
    """
    print(f"\n{'=' * 80}")
    print(f"Starting deduplication process for table: {table_name}")
    print(f"{'=' * 80}")

    # Read the Delta table
    print(f"Reading table {table_name}...")
    df = spark.read.format("delta").table(table_name)

    # Count total records
    total_records = df.count()
    print(f"Total records in table: {total_records}")

    # Check for duplicates based on ID
    duplicate_counts = df.groupBy(id_column).count().filter("count > 1")
    duplicate_ids_count = duplicate_counts.count()

    if duplicate_ids_count == 0:
        print(f"No duplicate IDs found in table {table_name}. No action needed.")
        return

    # Get some sample duplicate IDs for logging
    sample_duplicate_ids = duplicate_counts.limit(5).collect()
    print(f"Found {duplicate_ids_count} IDs with duplicates.")
    print("Sample duplicate IDs:")
    for row in sample_duplicate_ids:
        print(f"  ID: {row[id_column]}, Count: {row['count']}")

    # Create a window spec to rank records by timestamp within each ID group
    window_spec = Window.partitionBy(id_column).orderBy(col(timestamp_column).desc())

    # Add row numbers to identify the latest record for each ID
    df_with_rank = df.withColumn("row_num", row_number().over(window_spec))

    # Keep only the latest record for each ID
    deduplicated_df = df_with_rank.filter(col("row_num") == 1).drop("row_num")

    # Count deduplicated records
    deduplicated_count = deduplicated_df.count()
    removed_count = total_records - deduplicated_count

    print(f"Records after deduplication: {deduplicated_count}")
    print(f"Duplicates removed: {removed_count}")

    # Create temporary view to use for replacement
    temp_view_name = f"{table_name.replace('.', '_')}_deduplicated"
    deduplicated_df.createOrReplaceTempView(temp_view_name)

    # Overwrite the table with deduplicated data
    print(f"Overwriting table {table_name} with deduplicated data...")

    # Get the partition columns for the table
    partition_cols = [field.name for field in df.schema.fields
                      if field.name in ["Year", "Month", "Date", "Phone"]]

    print(f"Using partition columns: {partition_cols}")

    # Write back to the Delta table
    deduplicated_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy(*partition_cols) \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    print(f"Deduplication completed for table {table_name}")

    # Run VACUUM to clean up files (optional - comment out in production if needed)
    # print(f"Running VACUUM on table {table_name}...")
    # spark.sql(f"VACUUM {table_name}")


def deduplicate_delta_table_with_filter(spark, table_name, year=None, month=None, phone=None, id_column="Id",
                                        timestamp_column="TimeStamp"):
    """
    Deduplicate a Delta table by keeping the record with the latest timestamp for each ID,
    filtered by year, month, and optionally phone.

    Parameters:
    - spark: SparkSession
    - table_name: Name of the Delta table (e.g., "activity_dev.activity_transaction")
    - year: Filter for specific year (integer)
    - month: Filter for specific month (integer)
    - phone: Filter for specific phone (string)
    - id_column: Name of the column containing the unique identifier
    - timestamp_column: Name of the column containing the timestamp
    """
    print(f"\n{'=' * 80}")
    print(f"Starting filtered deduplication process for table: {table_name}")

    if year:
        print(f"Filtering by Year: {year}")
    if month:
        print(f"Filtering by Month: {month}")
    if phone:
        print(f"Filtering by Phone: {phone}")
    print(f"{'=' * 80}")

    # Read the Delta table
    print(f"Reading table {table_name}...")
    df = spark.read.format("delta").table(table_name)

    # Apply filters
    filter_conditions = []

    # Adjust column name based on table (transaction tables use uppercase, event tables lowercase)
    year_col = "Year" if "transaction" in table_name.lower() else "year"
    month_col = "Month" if "transaction" in table_name.lower() else "month"
    phone_col = "Phone" if "transaction" in table_name.lower() else "phone"

    if year:
        filter_conditions.append(f"{year_col} = {year}")
    if month:
        filter_conditions.append(f"{month_col} = {month}")
    if phone:
        filter_conditions.append(f"{phone_col} = '{phone}'")

    if filter_conditions:
        filter_expr = " AND ".join(filter_conditions)
        df = df.filter(filter_expr)
        print(f"Applied filter: {filter_expr}")

    # Count total records
    total_records = df.count()
    print(f"Total records after filtering: {total_records}")

    if total_records == 0:
        print(f"No records found with the specified filters. No action needed.")
        return

    # Check for duplicates based on ID
    duplicate_counts = df.groupBy(id_column).count().filter("count > 1")
    duplicate_ids_count = duplicate_counts.count()

    if duplicate_ids_count == 0:
        print(f"No duplicate IDs found in the filtered dataset. No action needed.")
        return

    # Get some sample duplicate IDs for logging
    sample_duplicate_ids = duplicate_counts.limit(5).collect()
    print(f"Found {duplicate_ids_count} IDs with duplicates.")
    print("Sample duplicate IDs:")
    for row in sample_duplicate_ids:
        print(f"  ID: {row[id_column]}, Count: {row['count']}")

    # Create a window spec to rank records by timestamp within each ID group
    window_spec = Window.partitionBy(id_column).orderBy(col(timestamp_column).desc())

    # Add row numbers to identify the latest record for each ID
    df_with_rank = df.withColumn("row_num", row_number().over(window_spec))

    # Keep only the latest record for each ID
    deduplicated_df = df_with_rank.filter(col("row_num") == 1).drop("row_num")

    # Count deduplicated records
    deduplicated_count = deduplicated_df.count()
    removed_count = total_records - deduplicated_count

    print(f"Records after deduplication: {deduplicated_count}")
    print(f"Duplicates removed: {removed_count}")

    # For all tables, we'll use the safer approach of rewriting the partitions
    print(f"Rewriting filtered data without duplicates...")

    # Get the partition columns for the table
    partition_cols = [field.name for field in df.schema.fields
                      if field.name.lower() in ["year", "month", "date", "phone"]]

    print(f"Using partition columns: {partition_cols}")

    # Define the conditions for the data to be replaced
    where_conditions = []
    if year:
        where_conditions.append(f"{year_col} = {year}")
    if month:
        where_conditions.append(f"{month_col} = {month}")
    if phone:
        where_conditions.append(f"{phone_col} = '{phone}'")

    # Construct the WHERE clause
    where_clause = " AND ".join(where_conditions)

    # The rewrite approach works by:
    # 1. Saving IDs we want to keep
    # 2. Deleting the specified partition data
    # 3. Inserting the deduplicated data

    # Create a temporary view with the IDs we want to keep
    deduplicated_df.createOrReplaceTempView("deduplicated_data")

    # Write to a temporary table first
    temp_table_name = f"{table_name}_temp_{int(datetime.now().timestamp())}"

    print(f"Creating temporary Delta table: {temp_table_name}")
    deduplicated_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(temp_table_name)

    # Delete data from the specified partition
    print(f"Deleting data from partitions where {where_clause}...")
    delete_sql = f"DELETE FROM {table_name} WHERE {where_clause}"
    spark.sql(delete_sql)

    # Insert the deduplicated data from the temp table
    print(f"Inserting deduplicated data...")
    insert_sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name}"
    spark.sql(insert_sql)

    # Drop the temporary table
    print(f"Cleaning up temporary table...")
    spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")

    print(f"Successfully completed filtered deduplication")


def perform_delta_table_analysis(spark, table_name, id_column="Id"):
    """
    Analyze a Delta table and report statistics related to duplication
    """
    print(f"\n{'=' * 80}")
    print(f"Analyzing table: {table_name}")
    print(f"{'=' * 80}")

    # Read the Delta table
    df = spark.read.format("delta").table(table_name)

    # Compute basic statistics
    total_records = df.count()
    distinct_ids = df.select(id_column).distinct().count()

    print(f"Total records: {total_records}")
    print(f"Distinct IDs: {distinct_ids}")
    print(f"Potential duplicates: {total_records - distinct_ids}")

    # Analyze duplication patterns
    id_counts = df.groupBy(id_column).count().cache()

    # Count entries with duplicates
    duplicate_counts = id_counts.filter("count > 1")
    duplicate_ids_count = duplicate_counts.count()

    if duplicate_ids_count > 0:
        max_dupes = duplicate_counts.agg(pyspark_max("count")).collect()[0][0]

        # Show distribution of duplicates
        print("\nDuplication distribution:")
        duplicate_counts.groupBy("count").count().orderBy("count").show(truncate=False)

        # Show some examples of the most duplicated IDs
        print(f"\nTop 5 most duplicated IDs:")
        duplicate_counts.orderBy(col("count").desc()).limit(5).show(truncate=False)

        print(f"\nSummary:")
        print(f"  {duplicate_ids_count} IDs have duplicates")
        print(f"  Maximum duplicates for a single ID: {max_dupes}")
    else:
        print("No duplicates found in the table.")

    id_counts.unpersist()


def main():
    """Main function to coordinate the deduplication process"""
    parser = argparse.ArgumentParser(description="Deduplicate Activity Delta tables")
    parser.add_argument("--table", choices=["transaction", "event", "both"], default="both",
                        help="Which table to deduplicate (transaction, event, or both)")
    parser.add_argument("--analyze-only", action="store_true",
                        help="Only analyze tables without performing deduplication")
    parser.add_argument("--year", type=int, help="Filter by specific year")
    parser.add_argument("--month", type=int, help="Filter by specific month")
    parser.add_argument("--phone", type=str, help="Filter by specific phone number")
    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()

    try:
        print(f"Starting Delta table deduplication process at {datetime.now()}")

        # Define table configurations
        tables = {
            "transaction": {
                "name": "activity_dev.activity_transaction",
                "id_column": "Id",
                "timestamp_column": "TimeStamp"
            },
            "event": {
                "name": "activity_dev.activity_event",
                "id_column": "id",
                "timestamp_column": "timestamp"
            }
        }

        # Determine which tables to process
        tables_to_process = []
        if args.table == "both":
            tables_to_process = ["transaction", "event"]
        else:
            tables_to_process = [args.table]

        # Check if filtering parameters are provided
        if args.year or args.month or args.phone:
            # If any filter parameter is provided
            if args.year is None or args.month is None:
                print("Error: When filtering, both --year and --month must be provided.")
                return

            # Process each table with filters
            for table_key in tables_to_process:
                table_config = tables[table_key]

                # Run analysis on the filtered table
                print(f"\nAnalyzing filtered data for {table_config['name']}...")

                # Apply filtering logic for analysis
                if args.analyze_only:
                    # For analyze-only, we just use standard analysis
                    # TODO: Add filtered analysis if needed
                    perform_delta_table_analysis(
                        spark,
                        table_config["name"],
                        table_config["id_column"]
                    )

                # Deduplicate with filters if not in analyze-only mode
                if not args.analyze_only:
                    deduplicate_delta_table_with_filter(
                        spark,
                        table_config["name"],
                        args.year,
                        args.month,
                        args.phone,
                        table_config["id_column"],
                        table_config["timestamp_column"]
                    )
        else:
            # No filtering parameters, process entire tables
            for table_key in tables_to_process:
                table_config = tables[table_key]

                # Run analysis on the table
                perform_delta_table_analysis(
                    spark,
                    table_config["name"],
                    table_config["id_column"]
                )

                # Deduplicate if not in analyze-only mode
                if not args.analyze_only:
                    deduplicate_delta_table(
                        spark,
                        table_config["name"],
                        table_config["id_column"],
                        table_config["timestamp_column"]
                    )

        print(f"\nDeduplication process completed at {datetime.now()}")

    except Exception as e:
        print(f"Error in deduplication process: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()