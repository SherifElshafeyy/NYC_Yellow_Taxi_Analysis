from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("sample_spark_job")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

df = spark.range(0, 10).toDF("number")
df.show()
spark.stop()