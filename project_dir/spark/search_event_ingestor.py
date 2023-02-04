from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName(
        "Analyzing the vocabulary of Pride and Prejudice."
    )
    .getOrCreate()
    )

