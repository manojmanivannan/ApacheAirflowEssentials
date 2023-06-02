from datetime import datetime, timedelta
#import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from logger.job_logger import logger as dag_logger


# local_tz = pendulum.timezone("Etc/UTC")
default_args = {
    'owner': 'manoj',
    'depends_on_past': False,
    'start_date': datetime.now().replace(hour=0,minute=0,second=0,microsecond=0), # tzinfo=local_tz),
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
kafka_bootstrap_servers = 'kafka:9092'
kafka_topic = 'transactions'
MAX_RECORDS_TO_READ = 20

def read_from_kafka(**kwargs):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id='kafka_connection',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='latest',
        enable_auto_commit=True,
    )
    
    with open(pyspark_app_home+'/progress.txt', 'r') as progress_file:
        progress = int(progress_file.read().strip())
    
    record_count=0
    # ti = kwargs['ti']

    for message in consumer:
        if record_count >= MAX_RECORDS_TO_READ: 
            dag_logger.warn(f'Read {MAX_RECORDS_TO_READ} records, breaking !')
            break

        if message.offset <= progress:
            dag_logger.warn(f'Skipping record as message offset:{message.offset} <= Progress: {progress} !')
            continue

        dag_logger.info(f'Record {record_count}: {message.value}')
        
        # Process each message (CSV record) as needed
        process_csv_record(message.value)
        record_count+=1
        
        
        # Update the progress in the progress file
        with open(pyspark_app_home+'/progress.txt', 'w') as progress_file:
            # dag_logger.warn(f'Updating progress with {message.offset}')
            progress_file.write(str(message.offset))

        
        # ti.xcom_push(key='progress', value=message.offset)

def process_csv_record(record):
    # Extract the values from the record
    name, address, phone, email = record.split(';')
    
    # Write the transformed values to a file
    with open(pyspark_app_home+'/transformed_data.txt', 'a') as file:
        file.write(f'{name};{address};{phone};{email}\n')

with DAG('Stream_Employee_Data_Loader',
         default_args=default_args,
         max_active_runs=2,
         catchup=False,
         schedule_interval='*/5 * * * *') as dag:
    
    read_kafka_task = PythonOperator(
        task_id='read_kafka',
        python_callable=read_from_kafka
    )
    
    
    load_employee_data = SparkSubmitOperator(task_id='load_employee_data',
        conn_id='spark_default',
        application=f'{pyspark_app_home}/stream_load_data.py',
        jars=jar_jdbc,
        driver_class_path=jar_jdbc,
        application_args=[postgres_url,postgres_user,postgres_pwd,postgres_db,postgres_table,pyspark_app_home+'/transformed_data.txt'],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        name='employee_data_getter',
        execution_timeout=timedelta(minutes=2),
    )
    
    # Define task dependencies
    read_kafka_task >> load_employee_data