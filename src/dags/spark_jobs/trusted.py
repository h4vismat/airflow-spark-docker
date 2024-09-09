from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .appName("trusted") \
        .getOrCreate()


df = spark.read.parquet("/home/airflow/refined.parquet")

df.write.format("delta").mode("overwrite").save("/home/airflow/delta/test")

spark.stop()
