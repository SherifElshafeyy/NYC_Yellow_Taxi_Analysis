from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "nyc_taxi_Cleaning",
    schedule_interval=None,  # Manual trigger for testing
    start_date=days_ago(1),
    catchup=False,
    description="Read and union all NYC taxi parquet files (2022-2025) into staging",
) as dag:

    read_and_union_years = SparkSubmitOperator(
        task_id="read_and_union_all_years",
        application="/opt/spark/app/nyc_taxi_read_all_years.py",
        name="nyc_taxi_union_all_years",
        conn_id="spark_default",
         verbose=False,
     )

    clean_unioned_data = SparkSubmitOperator(
        task_id="clean_unioned_data",
        application="/opt/spark/app/nyc_taxi_clean_data.py",
        name="nyc_taxi_clean_unioned_data",
        conn_id="spark_default",
        verbose=False,
    )

    read_and_union_years>>clean_unioned_data