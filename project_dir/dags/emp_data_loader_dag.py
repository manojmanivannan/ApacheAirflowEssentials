from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

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
pyspark_app_home    = "/opt/airflow/spark" # because local path ./project_dir/spark is mounted to /opt/airflow/spark. Ref docker-compose.yaml
postgres_url        = "jdbc:postgresql://postgres/airflow"
postgres_user       = "airflow"
postgres_pwd        = "airflow"
postgres_db         = "mdm"
postgres_table      = "emp_records"
jar_jdbc            = "/usr/local/share/postgresql-42.5.2.jar"

# Create a DAG
dag = DAG(dag_id='Employee_Data_Loader',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)


# STEP 1 #### Download the csv
download_employee_data = BashOperator(task_id='get_employee_data',dag=dag, bash_command=f"wget -O {pyspark_app_home}/employee.csv https://gist.githubusercontent.com/kevin336/acbb2271e66c10a5b73aacf82ca82784/raw/e38afe62e088394d61ed30884dd50a6826eee0a8/employees.csv")


# STEP 2 #### Load the csv into database
load_employee_data= SparkSubmitOperator(task_id='load_employee_data',
    conn_id='spark_default',
    application=f'{pyspark_app_home}/load_employee_data.py',
    jars=jar_jdbc,
    driver_class_path=jar_jdbc,
    application_args=[postgres_url,postgres_user,postgres_pwd,postgres_db,postgres_table],
    total_executor_cores=2,
    executor_cores=1,
    executor_memory='1g',
    driver_memory='1g',
    name='employee_data_getter',
    execution_timeout=timedelta(minutes=10),
    dag=dag,
    conf={
            # "spark.master": "local[*]",
            # "spark.dynamicAllocation.enabled": "false",
            # "spark.shuffle.service.enabled": "false",
        },
)

download_employee_data >> load_employee_data