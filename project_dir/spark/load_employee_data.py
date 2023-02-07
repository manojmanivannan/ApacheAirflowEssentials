from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Load Employee Data").getOrCreate()

df = spark.read.options(header='True',delimiter=',',)\
    .csv("/opt/airflow/spark/employee.csv")

df.show()
