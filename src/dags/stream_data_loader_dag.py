from datetime import datetime, timedelta
#import pendulum
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


consumer_logger = logging.getLogger("airflow")
# local_tz = pendulum.timezone("Etc/UTC")
default_args = {
    'owner': 'manoj',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1), # tzinfo=local_tz),
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

def read_from_kafka():
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

    for message in consumer:
        if record_count >= 10: 
            consumer_logger.info(f'Read 10 records, breaking !')
            break

        if message.offset <= progress: 
            continue

        consumer_logger.info(f'Record {record_count}: {message.value}')
        
        # Process each message (CSV record) as needed
        process_csv_record(message.value)
        record_count+=1
        
        
        # Update the progress in the progress file
        with open(pyspark_app_home+'/progress.txt', 'w') as progress_file:
            progress_file.write(str(message.offset))

def process_csv_record(record):
    # Assuming the CSV record has two columns: 'name' and 'age'
    # Extract the values from the record
    name, address, phone, email = record.split(';')
    
    # Perform a simple transformation
    # name = name.strip()  # Remove leading/trailing whitespace
    # ad = int(age)  # Convert age to an integer
    
    # Write the transformed values to a file
    with open(pyspark_app_home+'/transformed_data.txt', 'a') as file:
        file.write(f'{name};{address};{phone};{email}\n')

# STEP 2 #### Load the csv into database


with DAG('Stream_Employee_Data_Loader',
         default_args=default_args,
         schedule_interval='*/3 * * * *') as dag:
        #  schedule_interval=None) as dag:
    
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