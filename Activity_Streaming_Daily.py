from datetime import datetime, timedelta, date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
import json
from airflow.operators.bash import BashOperator
import time

default_args = {
    'owner': 'VM',
    'start_date': datetime(2023, 6, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=60)
}

with DAG('Activity_Streaming_Daily', default_args=default_args,
         schedule=None, catchup=False) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    begin_task = BashOperator(
        task_id="begin_task",
        bash_command="sleep 3",
    )

    Stream_Storage_Activity_Event = SparkSubmitOperator(task_id="Stream_Storage_Activity_Event",
                                        conn_id="spark_default",
                                        application="hdfs://192.168.10.167:9000/app/StorageActivityEvent.py",
                                        name="Stream_Storage_Activity_Event",
                                        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0",
                                        env_vars={"HADOOP_CONF_DIR":"/home/user/hadoop/etc/hadoop","SPARK_HOME":"/home/user/spark"},
                                        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
                                        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
                                        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
                                        driver_memory="512m",
                                        executor_memory="512m",
                                        executor_cores="1",
                                        num_executors="1"
                                        )


    Stream_Storage_Activity_Transaction = SparkSubmitOperator(task_id="Stream_Storage_Activity_Transaction",
                                        conn_id="spark_default",
                                        application="hdfs://192.168.10.167:9000/app/StorageActivityTransactions.py",
                                        name="Stream_Storage_Activity_Transaction",
                                        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0",
                                        env_vars={"HADOOP_CONF_DIR":"/home/user/hadoop/etc/hadoop","SPARK_HOME":"/home/user/spark"},
                                        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
                                        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
                                        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
                                        driver_memory="512m",
                                        executor_memory="512m",
                                        executor_cores="1",
                                        num_executors="1"
                                        )

    Stream_Sum_Point_Per_Profile = SparkSubmitOperator(task_id="Stream_Sum_Point_Per_Profile",
                                        conn_id="spark_default",
                                        application="hdfs://192.168.10.167:9000/app/StreamingSumTotalPointPerProfile.py",
                                        name="Stream_Sum_Point_Per_Profile",
                                        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
                                        env_vars={"HADOOP_CONF_DIR":"/home/user/hadoop/etc/hadoop","SPARK_HOME":"/home/user/spark"},
                                        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
                                        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
                                        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
                                        driver_memory="512m",
                                        executor_memory="512m",
                                        executor_cores="1",
                                        num_executors="1"
                                        )

    Stream_Sum_Point_Per_Profile_Yearly = SparkSubmitOperator(task_id="Stream_Sum_Point_Per_Profile_Yearly",
                                        conn_id="spark_default",
                                        application="hdfs://192.168.10.167:9000/app/StreamingSumTotalPointPerProfileYearly.py",
                                        name="Stream_Sum_Point_Per_Profile_Yearly",
                                        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
                                        env_vars={"HADOOP_CONF_DIR":"/home/user/hadoop/etc/hadoop","SPARK_HOME":"/home/user/spark"},
                                        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
                                        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
                                        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
                                        driver_memory="512m",
                                        executor_memory="512m",
                                        executor_cores="1",
                                        num_executors="1"
                                        )

    Stream_Sum_Point_Per_Profile_AllTime = SparkSubmitOperator(task_id="Stream_Sum_Point_Per_Profile_AllTime",
                                        conn_id="spark_default",
                                        application="hdfs://192.168.10.167:9000/app/StreamingSumTotalPointPerProfileAllTime.py",
                                        name="StreamingSumTotalPointPerProfileAllTime",
                                        packages="io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-hive_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
                                        env_vars={"HADOOP_CONF_DIR":"/home/user/hadoop/etc/hadoop","SPARK_HOME":"/home/user/spark"},
                                        archives="hdfs://192.168.10.167:9000/app/activityenv.tar.gz#activityenv",
                                        py_files="hdfs://192.168.10.167:9000/app/lib_activity.zip",
                                        conf={"spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./activityenv/bin/python"},
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

    begin_task >> [Stream_Storage_Activity_Event,Stream_Storage_Activity_Transaction, Stream_Sum_Point_Per_Profile, Stream_Sum_Point_Per_Profile_Yearly, Stream_Sum_Point_Per_Profile_AllTime] >> end_task

