from datetime import datetime, timedelta, date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
import json
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import time

default_args = {
    'owner': 'VM',
    'start_date': datetime(2025, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=60)
}

# Get current month and year for Redis sync
current_date = datetime.now()
current_month = current_date.month
current_year = current_date.year

with DAG('Activity_Batch_Profile_Level_Frequency', default_args=default_args,
         schedule="*/10 * * * *", catchup=False, dagrun_timeout=timedelta(minutes=10)) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    begin_task = BashOperator(
        task_id="begin_task",
        bash_command="sleep 3",
    )

    Ranking_Profile_AllTime = SparkSubmitOperator(
        task_id="Ranking_Profile_AllTime",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/RankingProfileAllTime.py",
        name="Ranking_Profile_Yearly",
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={
            "HADOOP_CONF_DIR":"/home/user/hadoop/etc/hadoop",
            "SPARK_HOME":"/home/user/spark"
        },
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"
        },
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    Update_Profile_Level = SparkSubmitOperator(
        task_id="Update_Profile_Level",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/UpdateProfileLevelInDurationTime.py",
        name="Update_Profile_Level",
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
        env_vars={
            "HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop",
            "SPARK_HOME": "/home/user/spark"
        },
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"
        },
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    Load_Redis_Profile = SparkSubmitOperator(
        task_id="Load_Redis_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/CloneProfileToRedis.py",
        name="Load_Redis_Profile",
        application_args=[
            "--month", str(current_month),
            "--year", str(current_year)
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={
            "HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop",
            "SPARK_HOME": "/home/user/spark"
        },
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"
        },
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    end_task = BashOperator(
        task_id="end_task",
        bash_command="sleep 3",
        trigger_rule="all_success",
    )

    # Set up task dependencies properly
    begin_task >> Ranking_Profile_AllTime >> Update_Profile_Level >> Load_Redis_Profile >> end_task