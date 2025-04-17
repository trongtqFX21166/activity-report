from datetime import datetime, timedelta, date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
import json
import requests
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.state import State
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import time

ENVIRONMENT = "dev"
TEAMS_WEBHOOK_URL = "https://vietmapcorp.webhook.office.com/webhookb2/2c61a90f-eade-4969-bf38-3a86bb53ba98@fc2e159c-528b-4132-b3c0-f43226646ad7/IncomingWebhook/d53e1a76223749e8b69511f91143da71/83abef7b-fb54-483d-9b8b-f40dbafc3dae/V2eq9knglJDrhloZiOhgNAEQ9JrpfvwPFCxCrQFA03Hb01"


def get_yesterday_date(**context):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return yesterday_str


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
    Check if the DAG has any failed tasks and send a notification if it failed
    """
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    execution_date = dag_run.execution_date.strftime('%Y-%m-%d %H:%M:%S')

    # Get all task instances for the current DAG run
    task_instances = dag_run.get_task_instances()

    # Check if any task failed
    failed_tasks = [ti for ti in task_instances if ti.state == State.FAILED]

    if failed_tasks or dag_run.state == State.FAILED:
        # Create task failure details
        failed_task_details = "\n".join([f"- {ti.task_id}" for ti in failed_tasks])

        # Create a message to send to Teams
        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "FF0000",
            "summary": f"❌ DAG {dag_id} failed",
            "sections": [
                {
                    "activityTitle": f"❌ Airflow DAG Alert: DAG Failed",
                    "activitySubtitle": f"DAG: {dag_id}",
                    "facts": [
                        {"name": "Execution Date", "value": execution_date},
                        {"name": "Environment", "value": ENVIRONMENT},
                        {"name": "Failed Tasks",
                         "value": failed_task_details if failed_tasks else "DAG trigger failure"}
                    ],
                    "text": "The DAG has failed. Please check the Airflow UI for more details.",
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
            print(f"DAG failure notification sent successfully. Status: {response.status_code}")
        except Exception as e:
            print(f"Failed to send DAG failure notification: {str(e)}")


default_args = {
    'owner': 'VM',
    'start_date': datetime(2024, 12, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
    'on_failure_callback': send_teams_alert  # Add Teams notification on failure
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

    # Add a final task to check DAG status and send notifications if needed
    dag_status_check = PythonOperator(
        task_id='dag_status_check',
        python_callable=check_dag_status,
        provide_context=True,
        trigger_rule='all_done',  # This will run regardless of upstream task status
    )

    # Set up task dependencies
    begin_task >> driving_report_task >> end_task >> dag_status_check