from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("MinIO Test") \
    .master("spark://spark-master:7077") \
    .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

df = spark.read.parquet("/opt/spark/resources/NYC_Yellow_Taxi_Trips/2022/yellow_tripdata_2022-01.parquet")
print(f"✅ Read {df.count()} rows")

df.write.mode("overwrite").parquet("s3a://test/test_data")
print("✅ Success! Written to MinIO")

spark.stop()