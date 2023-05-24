from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, DoubleType, TimestampType
import psycopg2
import sys

# Parameters
postgres_url = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
postgres_db = sys.argv[4]
postgres_table = sys.argv[5]

spark = SparkSession.builder.appName("Load Employee Data").getOrCreate()
# spark.jars.append("/opt/airflow/spark/postgresql-42.5.2.jar")

data = spark.conf.get('data')

df = spark.read.csv(data, header=False)

conn = psycopg2.connect(
    host="postgres",
    user=postgres_user,
    password=postgres_pwd
)
cur = conn.cursor()

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
print(f'Creating SCHEMA {postgres_db}')
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {postgres_db}")
conn.commit()

# Create the table
print(f'Creating TABLE {postgres_db}.{postgres_table}')
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {postgres_db}.{postgres_table} (
        id SERIAL PRIMARY KEY,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        {", ".join(columns)}
    )
""")
conn.commit()


(
    df.write
    .format("jdbc")
    .option("url", postgres_url)
    .option("dbtable", f"{postgres_db}.{postgres_table}")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("append") # explore different write methods
    .save()
)

spark.stop()
cur.close()
conn.close()