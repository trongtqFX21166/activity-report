from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2024, 4, 14),  # Start from today
    'start_date': datetime.now() - timedelta(minutes=5),  # Start 5 minutes ago
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Environment configuration - can be set as an Airflow variable
# Set Variable "environment" to either "dev" or "prod" in Airflow UI
ENV = "prod"

# Create the DAG
dag = DAG(
    'TrafficReport_LiveReport_Verification',
    default_args=default_args,
    description='DAG to verify live reports every 5 minutes',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    dagrun_timeout=timedelta(minutes=5)
)

# Define the start task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Define the main Spark job task
verify_reports_task = SparkSubmitOperator(
    task_id='verify_live_reports',
    conn_id='spark_default',  # Make sure this connection is configured in Airflow
    application='hdfs://192.168.10.167:9000/app/LiveReportVerificationJob.py',  # Update with actual path
    name=f'Live Report Verification ({ENV})',
    conf={
        "HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop",
        "SPARK_HOME": "/home/user/spark"
    },
    application_args=[
        "--env", ENV  # Pass environment to the script
    ],
    java_home=None,
    packages='org.postgresql:postgresql:42.3.1',  # Include PostgreSQL JDBC driver
    driver_memory='1g',
    executor_memory='1g',
    num_executors=1,
    executor_cores=1,
    dag=dag,
)

# Define the end task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> verify_reports_task >> end_task