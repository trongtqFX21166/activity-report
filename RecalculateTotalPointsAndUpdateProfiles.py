from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import pytz
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with MongoDB configurations"""
    return SparkSession.builder \
        .appName("total_points_yearly_recalculation") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()


def get_mongodb_connection():
    """Create MongoDB client connection"""
    try:
        client = MongoClient('mongodb://192.168.10.97:27017/')
        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        raise


def get_postgres_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host="192.168.8.230",  # Using the host from your sample code
        database="TrongTestDB1",  # Using database name from sample
        user="postgres",
        password="admin123."
    )


def get_all_years():
    """Get list of all years available in MongoDB collections"""
    try:
        client = get_mongodb_connection()
        db = client["activity_membershiptransactionyearly_dev"]

        # Get all collection names
        collections = db.list_collection_names()

        # Filter collections that represent years (assuming they're numeric)
        years = []
        for collection in collections:
            try:
                year = int(collection)
                years.append(year)
            except ValueError:
                # Skip collections that aren't numeric (not year collections)
                continue

        return sorted(years)
    except Exception as e:
        logger.error(f"Error getting available years: {str(e)}")
        raise


def read_yearly_data_from_mongo(spark, year):
    """Read membership transaction data for a specific year from MongoDB"""
    collection_name = f"{year}"
    logger.info(f"Reading data from MongoDB collection: {collection_name}")

    return spark.read \
        .format("mongodb") \
        .option("spark.mongodb.read.connection.uri",
                f"mongodb://192.168.10.97:27017/activity_membershiptransactionyearly_dev.{collection_name}") \
        .option("readConcern.level", "majority") \
        .load()


def process_profiles_batch(batch_data, batch_size=1000):
    """Process batch of profile updates to PostgreSQL"""
    conn = None
    try:
        conn = get_postgres_connection()

        update_query = """
            UPDATE public."Profiles"
            SET "TotalPoints" = %s,
                "LastModified" = %s,
                "LastModifiedBy" = %s
            WHERE "Phone" = %s
        """

        insert_query = """
            INSERT INTO public."Profiles"
            ("Id", "Phone", "Package", "StartDate", "MembershipCode", 
             "TotalPoints", "CreatedAt", "CreatedBy", "LastModified", "LastModifiedBy")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("Phone") DO UPDATE
            SET "TotalPoints" = EXCLUDED."TotalPoints",
                "LastModified" = EXCLUDED."LastModified",
                "LastModifiedBy" = EXCLUDED."LastModifiedBy"
        """

        current_time = datetime.now()
        modified_by = "BatchPointsCalculator"

        # Prepare data for batch upsert (using the newer-style upsert with conflict handling)
        batch_data_list = []
        for row in batch_data:
            batch_data_list.append(
                (
                    row["uuid"],  # Id
                    row["phone"],  # Phone
                    "Standard",  # Package (default)
                    current_time,  # StartDate
                    row["membershipcode"] if row.get("membershipcode") else "Level1",  # MembershipCode
                    row["totalpoints"],  # TotalPoints
                    current_time,  # CreatedAt
                    modified_by,  # CreatedBy
                    current_time,  # LastModified
                    modified_by  # LastModifiedBy
                )
            )

        with conn.cursor() as cur:
            execute_batch(cur, insert_query, batch_data_list, page_size=batch_size)
            conn.commit()

        logger.info(f"Successfully updated {len(batch_data_list)} profiles in PostgreSQL")

    except Exception as e:
        logger.error(f"Error processing batch update to PostgreSQL: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def recalculate_total_points_all_years():
    """Main process to recalculate total points from all years and update Profile table"""
    spark = None

    try:
        # Get all available years
        all_years = get_all_years()
        if not all_years:
            logger.info("No yearly collections found in MongoDB")
            return

        logger.info(f"Found collections for years: {all_years}")

        # Initialize Spark
        spark = create_spark_session()

        # Create empty DataFrame for accumulating results
        accumulated_df = None

        # Process each year
        for year in all_years:
            logger.info(f"Processing year: {year}")

            # Read data for this year
            year_df = read_yearly_data_from_mongo(spark, year)

            # Log record count for this year
            year_count = year_df.count()
            logger.info(f"Year {year}: {year_count} records")

            if year_count == 0:
                logger.info(f"No data for year {year}, skipping")
                continue

            # Select relevant columns
            year_df = year_df.select(
                "phone",
                "membershipcode",
                "totalpoints"
            )

            # Add to accumulated data
            if accumulated_df is None:
                accumulated_df = year_df
            else:
                # Union with existing data
                accumulated_df = accumulated_df.union(year_df)

        if accumulated_df is None:
            logger.info("No data found in any year collection")
            return

            # Group by phone and sum totalpoints
        profile_points_df = accumulated_df.groupBy("phone") \
            .agg(
            sum("totalpoints").alias("totalpoints"),
            first("membershipcode").alias("membershipcode")
        )

        # Handle null membership codes - set to Level1
        profile_points_df = profile_points_df.withColumn(
            "membershipcode",
            when(col("membershipcode").isNull(), "Level1").otherwise(col("membershipcode"))
        )

        # Add UUID column for Postgres ID
        profile_points_df = profile_points_df.withColumn(
            "uuid", expr("uuid()")
        )

        # Log total profiles to be updated
        total_profiles = profile_points_df.count()
        logger.info(f"Total profiles to update: {total_profiles}")

        # Process in batches
        batch_size = 500
        total_batches = (total_profiles + batch_size - 1) // batch_size

        logger.info(f"Will process in {total_batches} batches of {batch_size}")

        # Convert to rows and process in batches
        rows = profile_points_df.collect()
        profile_data = [row.asDict() for row in rows]

        for i in range(0, len(profile_data), batch_size):
            batch = profile_data[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}/{total_batches} ({len(batch)} profiles)")
            process_profiles_batch(batch, batch_size)

        logger.info("Profile updates completed successfully")

        # Show sample results
        logger.info("\nSample results (top 10 by points):")
        profile_points_df.orderBy(desc("totalpoints")).show(10, truncate=False)

    except Exception as e:
        logger.error(f"Error in recalculation process: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


def main():
    start_time = datetime.now()
    logger.info(f"Starting total points recalculation process at {start_time}")

    try:
        recalculate_total_points_all_years()
        logger.info(f"Recalculation process completed successfully")
    except Exception as e:
        logger.error(f"Recalculation process failed: {str(e)}")
        sys.exit(1)

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Process completed at {end_time} (duration: {duration})")


if __name__ == "__main__":
    main()