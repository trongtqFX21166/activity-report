from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, date


def create_spark_session():
    return SparkSession.builder \
        .appName("campaign_rule_flow_analysis") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.2.0," +
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.cassandra.connection.host", "192.168.8.165,192.168.8.166,192.168.8.183") \
        .getOrCreate()


def get_current_date_timestamp():
    """Get start and end timestamps for current date"""
    today = date.today()
    start_of_day = int(datetime(today.year, today.month, today.day).timestamp() * 1000)
    end_of_day = int(datetime(today.year, today.month, today.day, 23, 59, 59).timestamp() * 1000)
    return start_of_day, end_of_day, today


def write_to_cassandra(df, keyspace, table):
    """Write DataFrame to Cassandra"""
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .mode("append") \
        .save()


def analyze_rule_flow_performance():
    try:
        spark = create_spark_session()
        print(f"Starting campaign rule flow analysis at {datetime.now()}")

        # Get current date range
        start_timestamp, end_timestamp, today = get_current_date_timestamp()
        print(
            f"Analyzing data for period: {datetime.fromtimestamp(start_timestamp / 1000)} to {datetime.fromtimestamp(end_timestamp / 1000)}")

        # Read transactions directly from Delta table location
        transactions_df = spark.read \
            .format("delta") \
            .load("/activity_dev/bronze_table/activity_transaction") \
            .filter((col("TimeStamp") >= start_timestamp) &
                    (col("TimeStamp") <= end_timestamp))

        # Check if we have data for current date
        transaction_count = transactions_df.count()
        if transaction_count == 0:
            print(f"No data found for current date {today}")
            return

        print(f"Found {transaction_count} transactions for analysis")

        # 1. Campaign Level Analysis
        campaign_metrics = transactions_df.groupBy("CampaignId") \
            .agg(
            first("CampaignName").alias("campaign_name"),
            countDistinct("Phone").alias("total_profiles_evaluated"),
            count(when(col("Name") == "AddPoint", True)).alias("total_successful_actions"),
            sum(when(col("Name") == "AddPoint", col("Value"))).alias("total_points_awarded")
        )

        # 2. Rule Flow Analysis
        rule_metrics = transactions_df.groupBy(
            "CampaignId",
            "RuleId",
            "RuleName"
        ).agg(
            first("CampaignName").alias("campaign_name"),
            countDistinct("Phone").alias("profiles_evaluated"),
            count(when(col("Name").isNotNull(), True)).alias("total_actions"),
            count(when(col("Name") == "AddPoint", True)).alias("addpoint_actions"),
            sum(when(col("Name") == "AddPoint", col("Value"))).alias("points_awarded")
        )

        # Add rule sequence and date information
        window_spec = Window.partitionBy("CampaignId").orderBy("RuleId")

        cassandra_metrics = rule_metrics \
            .join(
            campaign_metrics.select(
                "CampaignId",
                "total_profiles_evaluated",
                "total_successful_actions",
                "total_points_awarded"
            ),
            "CampaignId"
        ) \
            .withColumn("rule_sequence", row_number().over(window_spec)) \
            .withColumn("profiles_not_matched",
                        col("profiles_evaluated") - col("total_actions")) \
            .withColumn("date", lit(today)) \
            .withColumn("month", lit(today.month)) \
            .withColumn("year", lit(today.year)) \
            .withColumn("timestamp", lit(int(datetime.now().timestamp() * 1000)))

        # Convert column names to lowercase for Cassandra
        cassandra_metrics = cassandra_metrics \
            .select(
            col("CampaignId").alias("campaignid"),
            col("campaign_name").alias("campaignname"),
            col("RuleId").alias("ruleid"),
            col("RuleName").alias("rulename"),
            col("date"),
            col("month"),
            col("year"),
            col("timestamp"),
            col("rule_sequence"),
            col("total_profiles_evaluated"),
            col("profiles_evaluated"),
            col("total_actions"),
            col("addpoint_actions"),
            col("points_awarded"),
            col("total_successful_actions"),
            col("total_points_awarded"),
            col("profiles_not_matched")
        )

        # Save to Cassandra
        write_to_cassandra(cassandra_metrics, "activity_dev", "daily_rule_flow_metrics")

        # Display Results
        print("\nCampaign Overview (Today):")
        cassandra_metrics.select(
            "campaignname",
            "total_profiles_evaluated",
            "total_successful_actions",
            "total_points_awarded"
        ).distinct().show(truncate=False)

        print("\nRule Flow Analysis (Today):")
        cassandra_metrics.select(
            "campaignname",
            "rulename",
            "rule_sequence",
            "profiles_evaluated",
            "total_actions",
            "addpoint_actions",
            "points_awarded",
            "profiles_not_matched"
        ).orderBy("campaignid", "rule_sequence").show(truncate=False)

        # Rule Flow Path Analysis
        print("\nRule Flow Path Analysis (Today):")
        for campaign_row in cassandra_metrics.select(
                "campaignid", "campaignname", "total_profiles_evaluated"
        ).distinct().collect():
            campaign_id = campaign_row["campaignid"]
            campaign_name = campaign_row["campaignname"]

            print(f"\nCampaign: {campaign_name}")
            campaign_rules = cassandra_metrics \
                .filter(col("campaignid") == campaign_id) \
                .orderBy("rule_sequence")

            total_profiles = campaign_row["total_profiles_evaluated"]
            remaining_profiles = total_profiles

            for rule in campaign_rules.collect():
                rule_name = rule["rulename"]
                profiles_matched = rule["total_actions"]
                remaining_profiles -= profiles_matched

                print(f"Rule {rule['rule_sequence']}: {rule_name}")
                print(f"  - Profiles Evaluated: {rule['profiles_evaluated']}")
                print(f"  - Total Actions: {profiles_matched}")
                print(f"  - AddPoint Actions: {rule['addpoint_actions']}")
                print(f"  - Points Awarded: {rule['points_awarded']}")
                print(f"  - Continued to Next Rule: {rule['profiles_not_matched']}")

        print(f"\nAnalysis completed at {datetime.now()}")
        print("Results saved to Cassandra: activity_dev.daily_rule_flow_metrics")

    except Exception as e:
        print(f"Error in rule flow analysis: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    analyze_rule_flow_performance()