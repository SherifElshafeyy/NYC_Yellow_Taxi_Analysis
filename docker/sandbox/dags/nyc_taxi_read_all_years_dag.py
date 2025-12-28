from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.operators.trino import TrinoOperator

with DAG(
    "nyc_taxi_Cleaning",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    description="Read and Standarize Schema of the NYC Taxi  Data (2022-2023-2024-2025) , Union Them and Clean Them For analysis"
) as dag:

    standarize_Schema_Union_Years = SparkSubmitOperator(
        task_id="Standarize_Schema_Union_Years",
        application="/opt/spark/app/nyc_taxi_read_all_years.py",
        name="nyc_taxi_union_all_years",
        conn_id="spark_default",
        verbose=False
     )

    clean_data = SparkSubmitOperator(
        task_id="clean_data",
        application="/opt/spark/app/nyc_taxi_clean_data.py",
        name="nyc_taxi_clean_unioned_data",
        conn_id="spark_default",
        verbose=False
    )

    create_trino_table = TrinoOperator(
        task_id="create_trino_cleaned_table",
        trino_conn_id="trino_default",
        sql="""
        CREATE TABLE IF NOT EXISTS hive.nyc.cleaned_data (
            Vendor_ID integer,
            Vendor_Name varchar,
            Trip_Pickup_DateTime timestamp,
            Trip_Dropoff_DateTime timestamp,
            passenger_count bigint,
            Pickup_Location_ID integer,
            Pickup_Borough varchar,
            Pickup_Zone varchar,
            Pickup_Service_Zone varchar,
            Dropoff_Location_ID integer,
            Dropoff_Borough varchar,
            Dropoff_Zone varchar,
            Dropoff_Service_Zone varchar,
            Trip_Distance_Km double,
            trip_distance_miles double,
            Ratecode_ID bigint,
            Ratecode_Description varchar,
            payment_type bigint,
            Payment_Method varchar,
            trip_duration_min double,
            fare_amount double,
            extra_charges double,
            mta_tax double,
            tip_amount double,
            tolls_amount double,
            improvement_surcharge double,
            total_amount double,
            congestion_surcharge double,
            Airport_fee double,
            cbd_congestion_fee double,
            Year integer,
            Month varchar
        )
        WITH (
            external_location = 's3a://nyc/cleaned_data',
            format = 'PARQUET'
        )
        """,
        autocommit=True
    )


    standarize_Schema_Union_Years>>clean_data>>create_trino_table