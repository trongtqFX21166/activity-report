from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ProfileRankUpdater")


# Environment-specific configurations
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
    """Create a Spark session with environment-specific configuration"""
    app_name = f"ProfileRankUpdater-{env}"

    return SparkSession.builder \
        .appName(app_name) \
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


def get_current_datetime_epoch():
    """Get current date and time information in seconds since epoch"""
    now = datetime.now()
    timestamp = int(time.time())
    date_timestamp = int(datetime(now.year, now.month, now.day).timestamp())

    return {
        "timestamp": timestamp,
        "date_timestamp": date_timestamp,
        "month": now.month,
        "year": now.year
    }


def fetch_profiles_from_postgres(env):
    """Fetch profiles directly from PostgreSQL"""
    logger.info(f"Fetching profiles from PostgreSQL ({env} environment)")

    conn = None
    try:
        conn = get_postgres_connection(env)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                'SELECT "Id", "Phone", "TotalPoints", "Rank", "MembershipCode" FROM public."Profiles"'
            )
            profiles = cursor.fetchall()

        logger.info(f"Fetched {len(profiles)} profiles from database")

        # Safely compute statistics
        try:
            if profiles:
                # Use a safer approach to extract points
                total_points = []
                for p in profiles:
                    try:
                        if p["TotalPoints"] is not None:
                            total_points.append(int(p["TotalPoints"]))
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Could not convert TotalPoints for profile {p['Id']}: {e}")

                if total_points:  # Only compute stats if we have valid points
                    min_points = min(total_points)
                    max_points = max(total_points)
                    avg_points = sum(total_points) / len(total_points)

                    logger.info(f"Points statistics - Min: {min_points}, Max: {max_points}, Avg: {avg_points:.2f}")

                    # Show top profiles by points
                    # Sort profiles based on TotalPoints, handling None values
                    def get_points(profile):
                        try:
                            return int(profile["TotalPoints"]) if profile["TotalPoints"] is not None else -1
                        except (TypeError, ValueError):
                            return -1

                    top_profiles = sorted(profiles, key=get_points, reverse=True)[:5]
                    logger.info("Top 5 profiles by points:")
                    for p in top_profiles:
                        logger.info(f"Phone: {p['Phone']}, Points: {p['TotalPoints']}, Current Rank: {p['Rank']}")
        except Exception as stats_error:
            logger.warning(f"Error calculating statistics: {stats_error}")
            # Continue processing even if statistics calculation fails

        return profiles
    except Exception as e:
        logger.error(f"Error fetching profiles: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def create_spark_dataframe_from_profiles(spark, profiles):
    """Convert Python list of profiles to Spark DataFrame, handling potential data issues"""
    logger.info("Converting profiles to Spark DataFrame")

    # Define schema for the DataFrame
    schema = StructType([
        StructField("Id", StringType(), False),
        StructField("Phone", StringType(), False),
        StructField("TotalPoints", IntegerType(), True),  # Allow null values
        StructField("Rank", IntegerType(), True),  # Allow null values
        StructField("MembershipCode", StringType(), True),
    ])

    # Convert list of dict to list of tuples, with defensive programming
    profile_data = []
    for p in profiles:
        try:
            # Handle potential missing or invalid data
            profile_id = str(p["Id"]) if p["Id"] is not None else ""
            phone = str(p["Phone"]) if p["Phone"] is not None else ""

            # Convert TotalPoints and Rank to int, defaulting to 0 if invalid
            try:
                total_points = int(p["TotalPoints"]) if p["TotalPoints"] is not None else 0
            except (ValueError, TypeError):
                logger.warning(f"Invalid TotalPoints for profile {profile_id}: {p['TotalPoints']}")
                total_points = 0

            try:
                rank = int(p["Rank"]) if p["Rank"] is not None else 9999999
            except (ValueError, TypeError):
                logger.warning(f"Invalid Rank for profile {profile_id}: {p['Rank']}")
                rank = 9999999

            membership_code = str(p["MembershipCode"]) if p["MembershipCode"] is not None else ""

            profile_data.append((profile_id, phone, total_points, rank, membership_code))
        except Exception as e:
            logger.warning(f"Error processing profile: {e}")
            # Skip this profile if there's an error

    # Create DataFrame
    profiles_df = spark.createDataFrame(profile_data, schema)
    df_count = profiles_df.count()
    logger.info(f"Created Spark DataFrame with {df_count} rows")

    # If no profiles were successfully processed, raise an error
    if df_count == 0:
        raise ValueError("No valid profiles could be processed")

    return profiles_df


def calculate_ranks_with_spark(spark, profiles_df):
    """Calculate ranks using Spark's window functions"""
    logger.info("Calculating ranks using Spark window functions")

    # Define window spec for ranking - order by total points descending
    window_spec = Window.orderBy(desc("TotalPoints"))

    # Apply rank function to calculate new ranks
    ranked_df = profiles_df.withColumn("new_rank", rank().over(window_spec))

    # Filter to only profiles that need updating
    updates_df = ranked_df.filter(col("new_rank") != col("Rank"))

    update_count = updates_df.count()
    logger.info(f"Found {update_count} profiles that need rank updates")

    # Show sample of changes
    if update_count > 0:
        logger.info("Sample of rank changes:")
        updates_df.select(
            "Id", "Phone", "TotalPoints", "Rank", "new_rank"
        ).orderBy("new_rank").show(10, truncate=False)

    return updates_df


def generate_update_batch(updates_df):
    """Generate batch update data from the Spark DataFrame"""
    if updates_df.count() == 0:
        logger.info("No updates to process")
        return []

    # Collect the data for updates
    updates = updates_df.select("Id", "new_rank").collect()
    logger.info(f"Preparing batch update for {len(updates)} profiles")

    # Create update batch data
    current_time = datetime.now()
    batch_data = []

    for row in updates:
        try:
            # Validate data before adding to batch
            if row["Id"] and row["new_rank"] is not None:
                batch_data.append((
                    int(row["new_rank"]),
                    current_time,
                    "ProfileRankUpdater",
                    row["Id"]
                ))
        except (ValueError, TypeError) as e:
            logger.warning(f"Error preparing update for profile {row['Id']}: {e}")

    logger.info(f"Generated {len(batch_data)} valid update statements")
    return batch_data


def update_postgres_batch(batch_data, env):
    """Execute batch update in PostgreSQL"""
    if not batch_data:
        logger.info("No updates to process")
        return 0

    update_count = len(batch_data)
    logger.info(f"Applying {update_count} rank updates to PostgreSQL in {env} environment")

    update_query = """
        UPDATE public."Profiles" 
        SET "Rank" = %s, 
            "LastModified" = %s, 
            "LastModifiedBy" = %s 
        WHERE "Id" = %s
    """

    conn = None
    try:
        conn = get_postgres_connection(env)
        with conn.cursor() as cursor:
            execute_batch(cursor, update_query, batch_data, page_size=1000)
        conn.commit()
        logger.info(f"Successfully updated {update_count} profile ranks")
        return update_count
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error updating profile ranks: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def update_ranks_with_spark(env):
    """Main function to update profile ranks using Spark"""
    start_time = time.time()
    spark = None

    try:
        # Initialize Spark session
        spark = create_spark_session(env)
        logger.info(f"Spark session initialized for {env} environment")

        # Fetch profiles from PostgreSQL
        profiles = fetch_profiles_from_postgres(env)

        # If no profiles found, exit early
        if not profiles:
            logger.info("No profiles found in the database. Nothing to process.")
            return

        # Convert to Spark DataFrame
        profiles_df = create_spark_dataframe_from_profiles(spark, profiles)

        # Calculate ranks using Spark window functions
        updates_df = calculate_ranks_with_spark(spark, profiles_df)

        # If no updates needed, exit early
        if updates_df.count() == 0:
            logger.info("No rank changes detected. No updates needed.")
            return

        # Generate batch update data
        batch_data = generate_update_batch(updates_df)

        # If no valid updates after validation, exit early
        if not batch_data:
            logger.info("No valid updates after data validation. Nothing to process.")
            return

        # Execute batch update in PostgreSQL
        updated_count = update_postgres_batch(batch_data, env)

        # Log performance stats
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Processing complete. Total updated: {updated_count}")
        logger.info(f"Total execution time: {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in profile rank update: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    # Determine environment
    env = get_environment()
    logger.info(f"Starting Profile Rank Update Process in {env.upper()} environment")

    try:
        # Update ranks using Spark
        update_ranks_with_spark(env)
        logger.info(f"Profile Rank Update Process completed successfully in {env.upper()} environment")
    except Exception as e:
        logger.error(f"Profile Rank Update Process failed in {env.upper()} environment: {str(e)}")
        raise


if __name__ == "__main__":
    main()