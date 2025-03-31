from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("create_table_activity_event") \
        .config("hive.metastore.uris", "thrift://192.168.10.167:9083") \
        .config("spark.sql.warehouse.dir", "/users/hive/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
        .enableHiveSupport() \
        .getOrCreate()

def create_activity_event_table():
    try:
        spark = create_spark_session()

        print("Creating database if not exists...")
        spark.sql("CREATE DATABASE IF NOT EXISTS activity_dev")

        print("Dropping existing table if exists...")
        spark.sql("DROP TABLE IF EXISTS activity_dev.activity_event")

        print("Creating activity_event table...")
        DeltaTable.createIfNotExists(spark) \
            .tableName("activity_dev.activity_event") \
            .addColumn("Id", "STRING", nullable=True) \
            .addColumn("Phone", "STRING", nullable=True) \
            .addColumn("Date", "LONG", nullable=True) \
            .addColumn("Month", "INT", nullable=True) \
            .addColumn("Year", "INT", nullable=True) \
            .addColumn("TimeStamp", "LONG", nullable=True) \
            .addColumn("MembershipCode", "STRING", nullable=True) \
            .addColumn("Event", "STRING", nullable=True) \
            .addColumn("ReportCode", "STRING", nullable=True) \
            .addColumn("EventName", "STRING", nullable=True) \
            .addColumn("ReportName", "STRING", nullable=True) \
            .addColumn("Data", "STRING", nullable=True) \
            .partitionedBy("Year", "Month", "Date", "Phone") \
            .location("/activity_dev/bronze_table/activity_event") \
            .property("description", "Activity Event bronze table") \
            .property("delta.logRetentionDuration", "interval 7 days") \
            .execute()

        print("Table created successfully!")

        print("\nTable Schema:")
        spark.sql("DESCRIBE activity_dev.activity_event").show(truncate=False)

        print("\nTable Properties:")
        spark.sql("SHOW TBLPROPERTIES activity_dev.activity_event").show(truncate=False)

    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    create_activity_event_table()