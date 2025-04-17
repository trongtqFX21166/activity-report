from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2024, 4, 14),  # Start from today
    'start_date': datetime(2024, 4, 15),  # Start 5 minutes ago
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Environment configuration - can be set as an Airflow variable
# Set Variable "environment" to either "dev" or "prod" in Airflow UI
ENV = "prod"

with DAG('TrafficReport_LiveReport_Verification', default_args=default_args,
         schedule="*/5 * * * *", catchup=False, dagrun_timeout=timedelta(minutes=5)) as dag:

    # Define the start task
    start_task = BashOperator(
        task_id='start',
        bash_command="sleep 3"
    )

    # Define the main Spark job task
    verify_reports_task = SparkSubmitOperator(
        task_id='verify_live_reports',
        conn_id='spark_default',  # Make sure this connection is configured in Airflow
        application='hdfs://192.168.10.167:9000/app/LiveReportVerificationJob.py',  # Update with actual path
        name=f'Live Report Verification ({ENV})',
        application_args=[
            "--env", str(ENV)
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory='512m',
        executor_memory='512m',
        num_executors=1,
        executor_cores=1
    )

    # Define the end task
    end_task = BashOperator(
        task_id='end',
        bash_command="sleep 3"
    )

    # Set task dependencies
    start_task >> verify_reports_task >> end_task