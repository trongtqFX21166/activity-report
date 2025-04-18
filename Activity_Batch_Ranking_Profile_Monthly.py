from datetime import datetime, timedelta, date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dateutil.relativedelta import relativedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
import json
import requests
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import time
from airflow.utils.timezone import make_aware
from airflow.utils.state import State

ENVIRONMENT = "prod"
TEAMS_WEBHOOK_URL = "https://vietmapcorp.webhook.office.com/webhookb2/2c61a90f-eade-4969-bf38-3a86bb53ba98@fc2e159c-528b-4132-b3c0-f43226646ad7/IncomingWebhook/d53e1a76223749e8b69511f91143da71/83abef7b-fb54-483d-9b8b-f40dbafc3dae/V2eq9knglJDrhloZiOhgNAEQ9JrpfvwPFCxCrQFA03Hb01"


# Function to get the previous month and year
def get_previous_month():
    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

    # prev_month = last_day_of_previous_month.month
    # prev_year = last_day_of_previous_month.year
    prev_month = 4
    prev_year = 2025

    return {"prev_month": prev_month, "prev_year": prev_year}


def send_teams_alert(context):
    """
    Send an alert to Microsoft Teams when a task fails
    """
    # Get information about the failed task
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    exception = context.get('exception')
    log_url = context.get('task_instance').log_url

    # Create a message to send to Teams
    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "FF0000",
        "summary": f"❌ DAG {dag_id} failed",
        "sections": [
            {
                "activityTitle": f"❌ Airflow DAG Alert: Task Failed",
                "activitySubtitle": f"DAG: {dag_id}",
                "facts": [
                    {"name": "Task", "value": task_id},
                    {"name": "Execution Date", "value": execution_date},
                    {"name": "Environment", "value": ENVIRONMENT}
                ],
                "text": f"Exception: {str(exception)}",
                "markdown": True
            }
        ],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "View Log",
                "targets": [{"os": "default", "uri": log_url}]
            }
        ]
    }

    # Send the message to Teams
    try:
        response = requests.post(
            TEAMS_WEBHOOK_URL,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print(f"Teams notification sent successfully. Status: {response.status_code}")
    except Exception as e:
        print(f"Failed to send Teams notification: {str(e)}")


def check_dag_status(**context):
    """
    Check if the DAG has any failed tasks and send a notification
    """
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    execution_date = dag_run.execution_date.strftime('%Y-%m-%d %H:%M:%S')

    # Get all task instances for the current DAG run
    task_instances = dag_run.get_task_instances()

    # Check if any task failed
    failed_tasks = [ti for ti in task_instances if ti.state == State.FAILED]

    # Determine if the DAG succeeded or failed
    if failed_tasks or dag_run.state == State.FAILED:
        status = "❌ Failed"
        theme_color = "FF0000"  # Red for failure

        # Create task failure details
        failed_task_details = "\n".join([f"- {ti.task_id}" for ti in failed_tasks])

        details_text = f"""
The DAG has failed. Please check the Airflow UI for more details.

Failed Tasks:
{failed_task_details if failed_tasks else "DAG trigger failure"}
"""
    else:
        status = "✅ Succeeded"
        theme_color = "00FF00"  # Green for success

        # Create monthly processing info
        prev_month_data = get_previous_month()
        month = prev_month_data["prev_month"]
        year = prev_month_data["prev_year"]

        details_text = f"""
The monthly ranking profile processing has completed successfully.

Processed data for month: {month}, year: {year}
"""

    # Create a message to send to Teams
    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": theme_color,
        "summary": f"{status}: DAG {dag_id}",
        "sections": [
            {
                "activityTitle": f"{status}: Monthly Ranking Profile Processing",
                "activitySubtitle": f"DAG: {dag_id}",
                "facts": [
                    {"name": "Execution Date", "value": execution_date},
                    {"name": "Environment", "value": ENVIRONMENT},
                    {"name": "Status", "value": status}
                ],
                "text": details_text,
                "markdown": True
            }
        ],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "View DAG",
                "targets": [{"os": "default", "uri": f"http://your-airflow-server/dags/{dag_id}/grid"}]
            }
        ]
    }

    # Send the message to Teams
    try:
        response = requests.post(
            TEAMS_WEBHOOK_URL,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print(f"DAG {status.lower()} notification sent successfully. Status: {response.status_code}")
    except Exception as e:
        print(f"Failed to send DAG notification: {str(e)}")


default_args = {
    'owner': 'VM',
    'start_date': make_aware(datetime(2025, 3, 31)),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_teams_alert  # Add Teams notification on failure
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
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}",
            "--env", str(ENVIRONMENT)
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

    # Ranking calculation job for previous month
    Ranking_Profile = SparkSubmitOperator(
        task_id="Ranking_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/RankingProfileMonthly.py",
        name="Ranking_Profile",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}",
            "--env", str(ENVIRONMENT)
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

    # Membership profile update for previous month
    Membership_Profile = SparkSubmitOperator(
        task_id="Membership_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/MembershipProfile.py",
        name="Membership_Profile",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}",
            "--env", str(ENVIRONMENT)
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

    # Load Redis cache for previous month
    Load_Redis = SparkSubmitOperator(
        task_id="Load_Redis",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/CloneMembershipMonthlyToRedis.py",
        name="Load_Redis",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_month') }}",
            "--year", "{{ ti.xcom_pull(task_ids='begin_task', key='prev_year') }}",
            "--env", str(ENVIRONMENT)
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
        executor_cores="4",
        num_executors="1"
    )

    Update_Membership_Profile = SparkSubmitOperator(
        task_id="Update_Membership_Profile",
        conn_id="spark_default",
        application="hdfs://192.168.10.167:9000/app/UpdateMembershipForProfile.py",
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
        executor_cores="4",
        num_executors="1"
    )

    # Add a final status check and notification task
    dag_status_check = PythonOperator(
        task_id="dag_status_check",
        python_callable=check_dag_status,
        provide_context=True,
        trigger_rule="all_done",  # Run this regardless of upstream task success/failure
    )

    # Success notification
    end_task = BashOperator(
        task_id="end_task",
        bash_command="echo 'Monthly activity ranking processing for {{ ti.xcom_pull(task_ids=\"begin_task\", key=\"prev_month\") }}/{{ ti.xcom_pull(task_ids=\"begin_task\", key=\"prev_year\") }} completed successfully at $(date)'",
        trigger_rule="all_success",
    )

    # Define task dependencies
    begin_task >> Recalculate_Profile >> Ranking_Profile >> Membership_Profile >> Load_Redis >> Clone_To_Next_Month >> Update_Membership_Profile >> end_task

    # Add the status check task as a separate branch from the end task
    end_task >> dag_status_check