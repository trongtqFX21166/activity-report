from datetime import datetime, timedelta, date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
import json
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import time

ENVIRONMENT = "prod"


def get_yesterday_date(**context):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return yesterday_str


default_args = {
    'owner': 'VM',
    'start_date': datetime(2024, 12, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=60)
}


with DAG('Activity_Push_Daily_Driving_Report', default_args=default_args,
         schedule="0 0 * * *", catchup=False, dagrun_timeout=timedelta(minutes=30)) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    begin_task = PythonOperator(
        task_id="begin_task",
        python_callable=get_yesterday_date,
        provide_context=True,
        do_xcom_push=True,
    )

    driving_report_task = SparkSubmitOperator(
        task_id="driving_report_task",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/DailyDrivingReportJob.py",
        name="Ranking_Profile",
        application_args=[
            '--date', '{{ ti.xcom_pull(task_ids="begin_task") }}',
            '--env', str(ENVIRONMENT)
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="4",
        num_executors="1"
    )

    end_task = BashOperator(
        task_id="end_task",
        bash_command="sleep 3",
    )

    # Set up task dependencies properly
    begin_task >> driving_report_task >> end_task