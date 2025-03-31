from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pymongo import MongoClient, UpdateOne
import os
import sys
import argparse
from datetime import datetime
import pytz

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-streaming_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'


def create_spark_session(app_name):
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.mongodb.read.connection.uri",
                "mongodb://192.168.10.97:27017/activity_membershiptransactionmonthly_dev") \
        .config("spark.mongodb.write.connection.uri",
                "mongodb://192.168.10.97:27017/activity_membershiptransactionmonthly_dev") \
        .config("spark.mongodb.read.database", "activity_membershiptransactionmonthly_dev") \
        .config("spark.mongodb.write.database", "activity_membershiptransactionmonthly_dev") \
        .enableHiveSupport() \
        .getOrCreate()


def get_membership_mapping():
    """Define membership levels based on rank percentiles"""
    return [
        (0.00, 0.01, "Level4", "Nổi tiếng"),  # Top 1%
        (0.01, 0.05, "Level3", "Uy tín"),  # Next 4%
        (0.05, 0.15, "Level2", "Ngôi sao đang lên"),  # Next 10%
        (0.15, 1.00, "Level1", "Nhập môn")  # Remaining 85%
    ]


def get_default_month_year():
    """Get current month and year in Vietnam timezone"""
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)
    return now.month, now.year


def validate_month_year(month, year):
    """Validate month and year parameters"""
    try:
        if not (1 <= month <= 12):
            raise ValueError(f"Month must be between 1 and 12, got {month}")

        # Create a date object to validate year (will raise ValueError for invalid year)
        datetime(year, month, 1)
        return True
    except ValueError as e:
        print(f"Error validating month/year: {str(e)}")
        return False


def assign_membership_levels(df):
    """Assign membership levels based on rank percentiles"""
    # Calculate total number of users
    total_users = df.count()

    if total_users == 0:
        print("No users found in the dataset")
        return df

    print(f"Total users for selected period: {total_users}")

    # Calculate rank first
    window_spec = Window.orderBy(desc("totalpoints"))
    df = df.withColumn("rank", row_number().over(window_spec))

    # Calculate rank percentile for each user
    ranked_df = df \
        .withColumn("percentile", col("rank") / total_users)

    # Apply membership rules based on percentiles
    mappings = get_membership_mapping()
    membership_conditions = []

    for min_perc, max_perc, code, name in mappings:
        condition = (
            when((col("percentile") > min_perc) & (col("percentile") <= max_perc),
                 struct(lit(code).alias("code"), lit(name).alias("name")))
        )
        membership_conditions.append(condition)

    membership_expr = coalesce(*membership_conditions)

    return ranked_df \
        .withColumn("membership", membership_expr) \
        .withColumn("membershipCode", col("membership.code")) \
        .withColumn("membershipName", col("membership.name")) \
        .drop("membership")


def update_membership_status(month=None, year=None):
    """Update membership status for specified month and year"""
    spark = None
    client = None

    try:
        # Get default month and year if not specified
        if month is None or year is None:
            month, year = get_default_month_year()

        # Validate month and year
        if not validate_month_year(month, year):
            print(f"Invalid month/year combination: {month}/{year}")
            return False

        print(f"Processing data for month: {month}, year: {year}")

        # Initialize Spark
        spark = create_spark_session(f"membership_rank_updater_{year}_{month}")

        # Initialize MongoDB client
        client = MongoClient('mongodb://192.168.10.97:27017/')
        db = client['activity_membershiptransactionmonthly_dev']
        collection_name = f"{year}_{month}"
        collection = db[collection_name]

        # Read specified month's data from MongoDB
        monthly_data = spark.read \
            .format("mongodb") \
            .option("uri", "mongodb://192.168.10.97:27017/activity_membershiptransactionmonthly_dev") \
            .option("database", "activity_membershiptransactionmonthly_dev") \
            .option("collection", collection_name) \
            .load()

        # Check if we have data for specified month
        record_count = monthly_data.count()
        if record_count == 0:
            print(f"No data found for month {month}, year {year}")
            return False

        print(f"Found {record_count} records to process")

        # Calculate and apply new membership levels
        membership_updates = assign_membership_levels(monthly_data)

        # Prepare final updates
        final_updates = membership_updates.select(
            col("phone"),
            col("membershipcode"),
            col("membershipname"),
            lit(month).alias("month"),
            lit(year).alias("year"),
            col("rank"),
            col("totalpoints"),
            col("timestamp"),
            current_timestamp().alias("processed_timestamp")
        )

        # Convert DataFrame to list of dictionaries for MongoDB
        updates = final_updates.collect()
        bulk_operations = []

        for update in updates:
            # Create update operation
            doc = update.asDict()
            filter_doc = {
                'phone': doc['phone'],
                'month': doc['month'],
                'year': doc['year']
            }
            update_doc = {'$set': doc}

            bulk_operations.append(
                UpdateOne(
                    filter_doc,
                    update_doc,
                    upsert=True
                )
            )

        # Create unique index on phone field for the collection if it doesn't exist
        try:
            collection.create_index([("phone", 1)], unique=True)
        except Exception as e:
            print(f"Note: Index creation - {str(e)}")

        # Execute bulk update
        if bulk_operations:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                print(f"MongoDB Update Results - Modified: {result.modified_count}, Upserted: {result.upserted_count}")
            except Exception as e:
                if "duplicate key error" in str(e):
                    print("Warning: Duplicate phone numbers detected. Handling duplicates...")
                    # Handle duplicates by keeping the record with the highest points
                    deduped_updates = final_updates.dropDuplicates(["phone"], "totalpoints")

                    # Create new bulk operations with deduplicated data
                    bulk_operations = []
                    for update in deduped_updates.collect():
                        doc = update.asDict()
                        bulk_operations.append(
                            UpdateOne(
                                {'phone': doc['phone']},
                                {'$set': doc},
                                upsert=True
                            )
                        )

                    # Retry bulk write with deduplicated data
                    result = collection.bulk_write(bulk_operations, ordered=False)
                    print(f"After deduplication - Modified: {result.modified_count}, Upserted: {result.upserted_count}")
                else:
                    raise

        # Show statistics
        print("\nMembership Level Distribution:")
        final_updates.groupBy("membershipCode", "membershipName") \
            .agg(
            count("*").alias("user_count"),
            round(avg("totalpoints"), 2).alias("avg_points"),
            round(min("totalpoints"), 2).alias("min_points"),
            round(max("totalpoints"), 2).alias("max_points")
        ) \
            .orderBy("membershipCode") \
            .show(truncate=False)

        # Show sample updates
        print("\nSample Records (sorted by points):")
        final_updates.orderBy(desc("totalpoints")) \
            .select(
            "phone",
            "membershipCode",
            "membershipName",
            "rank",
            "totalpoints"
        ) \
            .show(10, truncate=False)

        return True

    except Exception as e:
        print(f"Error updating membership status: {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()
        if client:
            client.close()


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process membership ranking for a specific month and year')
    parser.add_argument('--month', type=int, help='Month (1-12)')
    parser.add_argument('--year', type=int, help='Year (e.g., 2024)')

    args = parser.parse_args()

    # If one parameter is provided but not the other, use current date for missing parameter
    if (args.month is None and args.year is not None) or (args.month is not None and args.year is None):
        default_month, default_year = get_default_month_year()
        if args.month is None:
            args.month = default_month
            print(f"Month not specified, using current month: {args.month}")
        if args.year is None:
            args.year = default_year
            print(f"Year not specified, using current year: {args.year}")

    return args


def main():
    """Main entry point of the script"""
    # Parse command line arguments
    args = parse_arguments()

    # If no arguments provided, use current month and year
    if args.month is None and args.year is None:
        month, year = get_default_month_year()
        print(f"No month/year specified, using current date: {month}/{year}")
    else:
        month, year = args.month, args.year

    print(f"Starting membership status update process for {month}/{year}")
    success = update_membership_status(month, year)

    if success:
        print(f"Membership status update process completed successfully for {month}/{year}")
        sys.exit(0)
    else:
        print(f"Membership status update process failed for {month}/{year}")
        sys.exit(1)


if __name__ == "__main__":
    main()