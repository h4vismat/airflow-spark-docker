from pyspark.sql import SparkSession
from pyspark.sql import Row
import random
import uuid

spark = SparkSession.builder.master("local[*]") \
        .appName("raw") \
        .getOrCreate()

def generate_random_row():
    id = str(uuid.uuid4())
    random_number = random.random()

    return Row(a=id, b=random_number)

df = spark.createDataFrame([
    generate_random_row() for _ in range(10)
    ]
)

print("hello world")
df.write.mode("overwrite").csv('/home/airflow/raw.csv')

spark.stop()

