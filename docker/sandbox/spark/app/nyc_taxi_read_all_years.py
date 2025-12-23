from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, DoubleType,
    StringType, TimestampNTZType,
)
from pyspark.sql.functions import lit, col

standard_schema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', TimestampNTZType(), True),
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True),
    StructField('passenger_count', LongType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', LongType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', LongType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('Airport_fee', DoubleType(), True),
    StructField('cbd_congestion_fee', DoubleType(), True),
])

def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    if "airport_fee" in df.columns:
        df = df.withColumnRenamed("airport_fee", "Airport_fee")
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df.select([field.name for field in schema.fields])

def load_year_standardized(spark: SparkSession, year: int, max_month: int) -> DataFrame:
    base_path = f"/opt/spark/resources/NYC_Yellow_Taxi_Trips/{year}"
    dfs_std = {}
    for m in range(1, max_month + 1):
        month = f"{m:02d}"
        path = f"{base_path}/yellow_tripdata_{year}-{month}.parquet"
        df = spark.read.parquet(path)
        dfs_std[month] = enforce_schema(df, standard_schema)
    dfs_list = [dfs_std[month] for month in sorted(dfs_std.keys())]
    return reduce(lambda df1, df2: df1.unionByName(df2), dfs_list)

def main():
    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Union_All_Years")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .getOrCreate()
    )

    
    
        
        
    print("Loading year 2022...")
    df_2022_std = load_year_standardized(spark, 2022, 12)
    count_2022 = df_2022_std.count()
    print(f"2022 loaded: {count_2022} records")
        
    print("Loading year 2023...")
    df_2023_std = load_year_standardized(spark, 2023, 12)
    count_2023 = df_2023_std.count()
    print(f"2023 loaded: {count_2023} records")
        
    print("Loading year 2024...")
    df_2024_std = load_year_standardized(spark, 2024, 12)
    count_2024 = df_2024_std.count()
    print(f"2024 loaded: {count_2024} records")
        
    print("Loading year 2025...")
    df_2025_std = load_year_standardized(spark, 2025, 10)
    count_2025 = df_2025_std.count()
    print(f"2025 loaded: {count_2025} records")

    # Schema check
    schema_match = (df_2022_std.schema == df_2023_std.schema == 
                       df_2024_std.schema == df_2025_std.schema)
    print(f"Schema match: {schema_match}")

    # Union all years
    print("Unioning all years...")
    df_all_years_staged = (
        df_2024_std
        .unionByName(df_2025_std)
        .unionByName(df_2023_std)
        .unionByName(df_2022_std)
        )

    # Write to staging
    print("Writing to staging location...")
    df_all_years_staged.coalesce(1).write.mode("overwrite").parquet("/tmp/Staging_output")
        
    print("SUCCESS: Staging write completed")
    print(f"Total records: {count_2022 + count_2023 + count_2024 + count_2025}")
        
    
    spark.stop()

if __name__ == "__main__":
    main()