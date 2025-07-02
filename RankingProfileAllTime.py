from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import logging
import os
import sys
import time
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ProfileRankUpdater")


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


def get_postgres_config(env):
    """Return PostgreSQL configuration for the specified environment"""
    if env == 'dev':
        return {
            "host": "192.168.8.230",
            "database": "TrongTestDB1",
            "user": "postgres",
            "password": "admin123."
        }
    else:  # prod
        return {
            "host": "192.168.11.83",
            "database": "ActivityDB",
            "user": "vmladmin",
            "password": "5d6v6hiFGGns4onnZGW0VfKe"
        }


def create_spark_session(env):
    """Create Spark session with environment-specific configurations"""
    return SparkSession.builder \
        .appName(f"ProfileRankUpdater-AllTime-{env}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def get_postgres_connection(env):
    """Create a connection to PostgreSQL database using environment-specific config"""
    try:
        config = get_postgres_config(env)
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise


def fetch_profiles_from_postgres(env):
    """Fetch profiles directly from PostgreSQL"""
    logger.info(f"Fetching profiles from PostgreSQL ({env} environment)")

    conn = None
    try:
        conn = get_postgres_connection(env)
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT "Id", "Phone", "TotalPoints", "Rank" FROM public."Profiles"'
            )
            profiles = cursor.fetchall()

        logger.info(f"Fetched {len(profiles)} profiles from database")

        # Show statistics for points
        try:
            if profiles:
                points_list = [p[2] for p in profiles if p[2] is not None]
                if points_list:
                    min_points = min(points_list)
                    max_points = max(points_list)
                    avg_points = sum(points_list) / len(points_list)
                    logger.info(f"Points statistics - Min: {min_points}, Max: {max_points}, Avg: {avg_points:.2f}")

                    # Show top profiles by points
                    sorted_profiles = sorted(profiles, key=lambda p: p[2] if p[2] is not None else 0, reverse=True)
                    top_profiles = sorted_profiles[:5]
                    logger.info("Top 5 profiles by points:")
                    for p in top_profiles:
                        logger.info(f"Phone: {p[1]}, Points: {p[2]}, Current Rank: {p[3]}")
        except Exception as stats_error:
            logger.warning(f"Error calculating statistics: {stats_error}")

        return profiles
    except Exception as e:
        logger.error(f"Error fetching profiles: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def calculate_unique_ranks(profiles, env):
    """Calculate and update unique ranks for all profiles"""
    spark = None
    conn = None

    try:
        # Initialize Spark
        spark = create_spark_session(env)

        # Convert profiles list to DataFrame
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        schema = StructType([
            StructField("id", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("totalPoints", IntegerType(), True),
            StructField("rank", IntegerType(), True)
        ])

        # Handle potential None values for totalPoints
        profile_data = []
        for p in profiles:
            profile_data.append(
                (
                    str(p[0]),  # id
                    str(p[1]),  # phone
                    int(p[2]) if p[2] is not None else 0,  # totalPoints
                    int(p[3]) if p[3] is not None else 9999999  # rank
                )
            )

        df = spark.createDataFrame(profile_data, schema)

        # Log the count to verify data was loaded
        count = df.count()
        logger.info(f"Created DataFrame with {count} profiles")

        if count == 0:
            logger.info("No profiles to process")
            return []

        # Create window spec for ranking - similar to RankingProfileMonthly.py
        window_spec = Window.orderBy(
            desc("totalPoints"),
            asc("phone")  # Use phone as a tiebreaker for consistent ordering
        )

        # Calculate unique ranks
        ranked_df = df.withColumn("new_rank", row_number().over(window_spec))

        # Filter to only profiles that need updating
        updates_df = ranked_df.filter(col("new_rank") != col("rank"))

        # Get update count
        update_count = updates_df.count()
        logger.info(f"Found {update_count} profiles that need rank updates")

        # Show sample of rank changes
        if update_count > 0:
            logger.info("\nSample of rank changes:")
            updates_df.select(
                "phone",
                "totalPoints",
                "rank",
                "new_rank"
            ).orderBy("new_rank").show(10, truncate=False)

            # Check for duplicate ranks - should be none due to row_number()
            duplicate_ranks = ranked_df.groupBy("new_rank").count().filter(col("count") > 1)
            if duplicate_ranks.count() > 0:
                logger.warning("Warning: Duplicate ranks found!")
                duplicate_ranks.show()
            else:
                logger.info("Success: All ranks are unique!")

        # Return updates as a list of tuples (id, new_rank)
        updates = updates_df.select("id", "new_rank").collect()
        return [(row["id"], row["new_rank"]) for row in updates]

    except Exception as e:
        logger.error(f"Error calculating unique ranks: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


def update_ranks_in_postgres(updates, env):
    """Update ranks in PostgreSQL with deadlock prevention"""
    if not updates:
        logger.info("No rank updates to apply")
        return 0

    conn = None
    try:
        conn = get_postgres_connection(env)

        # Set a short transaction timeout to prevent long-running deadlocks
        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30s'")

        # Prepare update query
        update_query = """
            UPDATE public."Profiles"
            SET "Rank" = %s,
                "LastModified" = %s,
                "LastModifiedBy" = %s
            WHERE "Id" = %s
        """

        # Prepare batch data
        current_time = datetime.now()
        modified_by = "ProfileRankUpdater"

        batch_data = []
        for profile_id, new_rank in updates:
            batch_data.append(
                (
                    new_rank,
                    current_time,
                    modified_by,
                    profile_id
                )
            )

        # Process updates in smaller batches to reduce deadlock probability
        batch_size = 100
        total_updated = 0
        max_retries = 3

        # Randomize the order of updates to help prevent deadlocks
        random.shuffle(batch_data)
        logger.info(f"Processing {len(batch_data)} updates in batches of {batch_size}")

        for i in range(0, len(batch_data), batch_size):
            current_batch = batch_data[i:i + batch_size]
            retries = 0
            success = False

            # Retry logic for each batch
            while not success and retries < max_retries:
                try:
                    with conn.cursor() as cursor:
                        execute_batch(cursor, update_query, current_batch, page_size=batch_size)
                    conn.commit()
                    batch_updated = len(current_batch)
                    total_updated += batch_updated
                    logger.info(f"Batch {i // batch_size + 1}: Successfully updated {batch_updated} profile ranks")
                    success = True
                except psycopg2.errors.DeadlockDetected as e:
                    retries += 1
                    conn.rollback()
                    logger.warning(f"Deadlock detected in batch {i // batch_size + 1}, retry {retries}/{max_retries}")

                    # Add random delay before retry to help prevent deadlocks
                    sleep_time = 1 + (random.random() * retries)
                    time.sleep(sleep_time)

                    # Shuffle the batch to change the order of updates
                    random.shuffle(current_batch)
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error updating ranks in batch {i // batch_size + 1}: {str(e)}")
                    raise

            if not success:
                logger.error(f"Failed to update batch {i // batch_size + 1} after {max_retries} retries")

        logger.info(f"Successfully updated {total_updated} profile ranks in PostgreSQL")
        return total_updated

    except Exception as e:
        logger.error(f"Error updating ranks in PostgreSQL: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def main():
    """Main entry point of the script"""
    start_time = datetime.now()

    try:
        # Determine environment
        env = get_environment()
        logger.info(f"Starting Profile AllTime Ranking Process at {start_time} in {env.upper()} environment")

        # Fetch profiles from PostgreSQL
        profiles = fetch_profiles_from_postgres(env)

        if not profiles:
            logger.info("No profiles found in database. Nothing to process.")
            return

        # Calculate ranks
        rank_updates = calculate_unique_ranks(profiles, env)

        # Update ranks in PostgreSQL
        if rank_updates:
            updated_count = update_ranks_in_postgres(rank_updates, env)
            logger.info(f"Updated {updated_count} profile ranks")
        else:
            logger.info("No rank changes needed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Profile AllTime Ranking Process Completed at {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Profile AllTime Ranking Process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()