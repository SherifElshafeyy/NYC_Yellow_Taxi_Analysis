from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.trino.operators.trino import TrinoOperator


with DAG(
    "test_terino_operator",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:


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
