from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]") \
        .appName("refined") \
        .getOrCreate()


df = spark.read.csv("/home/airflow/raw.csv")

df.write.mode("overwrite").parquet('/home/airflow/refined.parquet')

spark.stop()

