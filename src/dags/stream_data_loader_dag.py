from datetime import datetime, timedelta
import os, json
import pendulum
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import functools

consumer_logger = logging.getLogger("airflow")
local_tz = pendulum.timezone("Etc/UTC")
default_args = {
    'owner': 'manoj',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10, tzinfo=local_tz),
    # 'email': ['example@example.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# pyspark_app_home=Variable("PYSPARK_APP_HOME")
pyspark_app_home    = "/opt/airflow/spark" # because local path ./src/spark is mounted to /opt/airflow/spark. Ref docker-compose.yaml
postgres_url        = "jdbc:postgresql://postgres/airflow"
postgres_user       = "airflow"
postgres_pwd        = "airflow"
postgres_db         = "mdm"
postgres_table      = "emp_records"
jar_jdbc            = "/usr/local/share/postgresql-42.5.2.jar"
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'transactions'

# Create a DAG
dag = DAG(dag_id='Stream_Employee_Data_Loader',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)

def process_message(ti, **context):
    message = ti.xcom_pull(task_ids='consume_records', key='message')
    # Process the message or perform any required transformations
    print(message)
    processed_data = message.upper()
    
    ti.xcom_push(key='processed_data', value=processed_data)

connection_config = {
    "bootstrap.servers": "localhost:9092",
}

def load_connections():
    # Connections needed for this example dag to finish
    from airflow.models import Connection
    from airflow.utils import db

    db.merge_conn(
        Connection(
            conn_id="kafka_connection",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "kafka_connection",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                }
            ),
        )
    )

def consumer_function(messages, prefix=None):
    consumer_logger.info("starting consumer function")
    consumer_logger.info(f"Messages {messages}")
    for message in messages:
        key = json.loads(message.key())
        value = json.loads(message.value())
        consumer_logger.info(
            f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}"
        )
    return
    
#establish_connection = PythonOperator(task_id="load_connections", python_callable=load_connections, dag=dag)

consume_task = ConsumeFromTopicOperator(
        task_id='consume_records',
        topics=[kafka_topic],
        apply_function_batch=functools.partial(
            consumer_function, prefix="consumed:::"
        ),
        consumer_config={
            "bootstrap.servers": "localhost:9092",
            "group.id": "kafka_connection",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
        dag=dag
    )

# retrieve_data_task = PythonOperator(
#         task_id='retrieve_data',
#         python_callable=process_message,
#         provide_context=True,
#         do_xcom_push=True,
#         dag=dag
#     )

# def submit_spark_job(ti, **context):
    # message = ti.xcom_pull(task_ids='consume_records',key="message")
spark_submit_task = SparkSubmitOperator(
        task_id='submit_spark_job',
        application=f'{pyspark_app_home}/stream_load_data.py',
        jars=jar_jdbc,
        driver_class_path=jar_jdbc,
        application_args=[postgres_url,postgres_user,postgres_pwd,postgres_db,postgres_table],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        execution_timeout=timedelta(minutes=10),
        dag=dag)

    # return spark_submit_task.execute(context=context)

# submit_task = PythonOperator(
#     task_id='submit_task',
#     python_callable=submit_spark_job,
#     provide_context=True,
#     dag=dag
# )

consume_task  >> spark_submit_task