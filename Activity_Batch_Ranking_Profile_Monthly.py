from datetime import datetime, timedelta, date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dateutil.relativedelta import relativedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
import json
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import time


# Function to get the previous month and year
def get_previous_month():
    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

    prev_month = last_day_of_previous_month.month
    prev_year = last_day_of_previous_month.year

    return {"prev_month": prev_month, "prev_year": prev_year}


default_args = {
    'owner': 'VM',
    'start_date': datetime(2023, 6, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Schedule to run at 1 AM on the 1st day of every month
with DAG('Activity_Batch_Ranking_Profile_Monthly',
         default_args=default_args,
         description='Process activity ranking data for the previous month',
         schedule_interval='0 1 1 * *',  # Run at 1 AM on the 1st day of each month
         catchup=False) as dag:
    # Task to set up processing month info and calculate current month
    def setup_processing_months(**context):
        # Get previous month data (for processing)
        previous_month_data = get_previous_month()
        prev_month = previous_month_data["prev_month"]
        prev_year = previous_month_data["prev_year"]

        # Get current month data (for cloning target)
        current_date = datetime.now()
        curr_month = current_date.month
        curr_year = current_date.year

        print(f"Processing data for month: {prev_month}, year: {prev_year}")
        print(f"Current month/year (for cloning): {curr_month}, {curr_year}")

        # Set month and year values as XCom for downstream tasks
        context['ti'].xcom_push(key='prev_month', value=prev_month)
        context['ti'].xcom_push(key='prev_year', value=prev_year)
        context['ti'].xcom_push(key='curr_month', value=curr_month)
        context['ti'].xcom_push(key='curr_year', value=curr_year)

        return {
            "prev_month": prev_month,
            "prev_year": prev_year,
            "curr_month": curr_month,
            "curr_year": curr_year
        }


    begin_task = PythonOperator(
        task_id="begin_task",
        python_callable=setup_processing_months,
        provide_context=True,
    )

    Recalculate_Profile = SparkSubmitOperator(
        task_id="Recalculate_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/RecalculateTotalPointPerProfileMonthly.py",
        name="Recalculate_Profile",
        application_args=[
            "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}"
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    # Ranking calculation job for previous month
    Ranking_Profile = SparkSubmitOperator(
        task_id="Ranking_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/RankingProfileMonthly.py",
        name="Ranking_Profile",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}"
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    # Membership profile update for previous month
    Membership_Profile = SparkSubmitOperator(
        task_id="Membership_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/MembershipProfile.py",
        name="Membership_Profile",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}"
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    # Load Redis cache for previous month
    Load_Redis = SparkSubmitOperator(
        task_id="Load_Redis",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/CloneMembershipMonthlyToRedis.py",
        name="Load_Redis",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}"
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    # Clone previous month data to current month for continuity
    Clone_To_Next_Month = SparkSubmitOperator(
        task_id="Clone_To_Next_Month",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/CloneMembershipToNextMonth.py",
        name="Clone_To_Next_Month",
        application_args=[
            "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}",
            "{{ ti.xcom_pull(task_ids='begin_task', key='curr_month') }}",
            "{{ ti.xcom_pull(task_ids='begin_task', key='curr_year') }}"
        ],
        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
        env_vars={"HADOOP_CONF_DIR": "/home/user/hadoop/etc/hadoop", "SPARK_HOME": "/home/user/spark"},
        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
        driver_memory="512m",
        executor_memory="512m",
        executor_cores="1",
        num_executors="1"
    )

    # Success notification
    end_task = BashOperator(
        task_id="end_task",
        bash_command="echo 'Monthly activity ranking processing for {{ ti.xcom_pull(task_ids=\"begin_task\", key=\"prev_month\") }}/{{ ti.xcom_pull(task_ids=\"begin_task\", key=\"prev_year\") }} completed successfully at $(date)'",
        trigger_rule="all_success",
    )

    # Define task dependencies
    begin_task >> Recalculate_Profile >> Ranking_Profile >> Membership_Profile >> Load_Redis >> Clone_To_Next_Month >> end_task