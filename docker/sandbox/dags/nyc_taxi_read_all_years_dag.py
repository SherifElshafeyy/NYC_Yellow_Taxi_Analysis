from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "nyc_taxi_Cleaning",
    schedule_interval=None,  # Manual trigger for testing
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

    standarize_Schema_Union_Years>>clean_data