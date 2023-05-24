from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

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




def process_message(ti):
    message = ti.xcom_pull(key='message')
    value = ti.value().decode('utf-8')
    return value

consume_task = ConsumeFromTopicOperator(
        task_id='consume_records',
        # bootstrap_servers=kafka_bootstrap_servers,
        topics=kafka_topic,
        apply_function=process_message, #'stream_data_loader_dag.process_message',
        dag=dag
    )

retrieve_data_task = PythonOperator(
        task_id='retrieve_data',
        python_callable=lambda: ti.xcom_pull(task_ids='Stream_Employee_Data_Loader.consume_records'),
        provide_context=True,
        dag=dag
    )

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
        conf={'data': '{{ task_instance.xcom_pull(task_ids="retrieve_data") }}'},
        execution_timeout=timedelta(minutes=10),
        dag=dag)

consume_task >> retrieve_data_task >> spark_submit_task