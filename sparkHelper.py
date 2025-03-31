
from pyspark.sql import  SparkSession
from delta import *

def init_spark_local(app_Name='spark_app'):
    builder = SparkSession \
        .builder \
        .appName(app_Name) \
        .master("local[20]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def init_spark_prod(app_Name='spark_app'):
    builder = SparkSession \
        .builder \
        .appName(app_Name) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def init_spark_cassandra_local(app_Name='spark_app'):
    builder = SparkSession \
        .builder \
        .appName(app_Name) \
        .master("local[20]") \
        .config("spark.cassandra.connection.host", "192.168.8.165,192.168.8.166,192.168.8.167") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions, io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport()

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def init_spark_cassandra_prod(app_Name='spark_app'):
    builder = SparkSession \
        .builder \
        .appName(app_Name) \
        .config("spark.cassandra.connection.host", "192.168.8.165,192.168.8.166,192.168.8.183") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions, io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport()

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark