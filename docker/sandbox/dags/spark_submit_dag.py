from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "test_spark_submit_operator",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="spark_submit_task",
        application="/opt/spark/app/sample_spark_job.py",
        name="spark_test_job",
        deploy_mode="client",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )