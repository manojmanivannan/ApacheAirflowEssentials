from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, DoubleType, TimestampType
import psycopg2

spark = SparkSession.builder.appName("Load Employee Data").getOrCreate()
# spark.jars.append("/opt/airflow/spark/postgresql-42.5.2.jar")

df = spark.read.options(header='True',delimiter=',',inferSchema='True').csv("/opt/airflow/spark/employee.csv")

df.show(5)

conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)
cur = conn.cursor()

# Create the database and table if they don't exist
table_name = "data"


# Get the schema of the DataFrame
schema = df.schema
columns = []
for field in schema:
    data_type = "text"
    if field.dataType == IntegerType():
        data_type = "integer"
    elif field.dataType == FloatType():
        data_type = "real"
    elif field.dataType == DoubleType():
        data_type = "double precision"
    elif field.dataType == TimestampType():
        data_type = "timestamp"
    columns.append(f"{field.name} {data_type}")

# create the schema
cur.execute("CREATE SCHEMA IF NOT EXISTS airflow")
conn.commit()

# Create the table
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS airflow.data (
        {", ".join(columns)}
    )
""")
conn.commit()


mode = "overwrite"
url = "jdbc:postgresql://postgres/airflow"
properties = {"user": "airflow","password": "airflow"}
df.write.jdbc(url=url, table="airflow.data", mode=mode, properties=properties)

(
    df.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "airflow.data")
    .option("user", "airflow")
    .option("password", "airflow")
    .mode("overwrite")
    .save()
)

spark.stop()
cur.close()
conn.close()