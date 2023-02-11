from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Asia/Tehran")
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

dag = DAG(dag_id='Employee_Data_Loader',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)



download_employee_data = BashOperator(task_id='get_employee_data',dag=dag, bash_command="wget -O /opt/airflow/spark/employee.csv https://gist.githubusercontent.com/kevin336/acbb2271e66c10a5b73aacf82ca82784/raw/e38afe62e088394d61ed30884dd50a6826eee0a8/employees.csv")

# pyspark_app_home=Variable("PYSPARK_APP_HOME")
pyspark_app_home='/opt/airflow/spark' # because local path ./project_dir/spark is mounted to /opt/airflow/spark. Ref docker-compose.yaml


load_employee_data= SparkSubmitOperator(task_id='load_employee_data',
    conn_id='spark_default',
    application=f'{pyspark_app_home}/load_employee_data.py',
    jars='/opt/airflow/spark/postgresql-42.5.2.jar',
    driver_class_path='/opt/airflow/spark/postgresql-42.5.2.jar',
    total_executor_cores=2,
    executor_cores=1,
    executor_memory='1g',
    driver_memory='1g',
    name='employee_data_getter',
    execution_timeout=timedelta(minutes=10),
    dag=dag,
    conf={
            "spark.master": "local[*]",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
        },
)

download_employee_data >> load_employee_data