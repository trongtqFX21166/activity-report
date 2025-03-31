from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def create_spark_session():
    """Create a Spark session that can read from Delta tables"""
    return SparkSession.builder \
        .appName("query_activity_transactions") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .enableHiveSupport() \
        .getOrCreate()


def query_activity_transactions(phone=None, year=None, month=None):
    """
    Query activity transactions with filter parameters

    Parameters:
    phone (str): Phone number to filter by
    year (int): Year to filter by
    month (int): Month to filter by

    Note: Any parameter can be None, in which case it won't be used for filtering
    """
    spark = create_spark_session()

    try:
        # Read from the Delta table
        df = spark.read.format("delta").table("activity_dev.activity_transaction")

        # Apply filters if parameters are provided
        if phone:
            df = df.filter(col("Phone") == phone)

        if year:
            df = df.filter(col("Year") == year)

        if month:
            df = df.filter(col("Month") == month)

        # Print the count of matching records
        count = df.count()
        print(f"Found {count} records matching the specified criteria")

        # Show the data
        if count > 0:
            # Select relevant columns and show results
            result = df.select(
                "Id",
                "Phone",
                "Date",
                "Month",
                "Year",
                "EventCode",
                "ReportCode",
                "Name",
                "Value",
                "TimeStamp",
                "MembershipCode",
                "Type"
            ).orderBy("TimeStamp", descending=True)

            # Show the first 20 rows
            result.show(20, truncate=False)

            return result
        else:
            print("No records found with the specified criteria")
            return None

    except Exception as e:
        print(f"Error querying transactions: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    # Parse command line arguments
    phone = None
    year = None
    month = None

    # Check if arguments were provided
    if len(sys.argv) > 1:
        # Loop through arguments
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--phone" and i + 1 < len(sys.argv):
                phone = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--year" and i + 1 < len(sys.argv):
                try:
                    year = int(sys.argv[i + 1])
                except ValueError:
                    print(f"Error: Year must be an integer, got {sys.argv[i + 1]}")
                    sys.exit(1)
                i += 2
            elif sys.argv[i] == "--month" and i + 1 < len(sys.argv):
                try:
                    month = int(sys.argv[i + 1])
                except ValueError:
                    print(f"Error: Month must be an integer, got {sys.argv[i + 1]}")
                    sys.exit(1)
                i += 2
            else:
                print(f"Unknown argument: {sys.argv[i]}")
                i += 1

    # If no arguments were provided, show usage
    if len(sys.argv) <= 1:
        print("Usage: python query_activity_transactions.py [--phone PHONE] [--year YEAR] [--month MONTH]")
        print("Examples:")
        print("  python query_activity_transactions.py --phone 933111113 --year 2024 --month 12")
        print("  python query_activity_transactions.py --year 2024 --month 12")
        print("  python query_activity_transactions.py --phone 933111113")
        sys.exit(0)

    # Print query parameters
    print(f"Querying with parameters: phone={phone}, year={year}, month={month}")

    # Execute the query
    query_activity_transactions(phone=phone, year=year, month=month)