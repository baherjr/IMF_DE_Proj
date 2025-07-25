from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="imf_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL pipeline with SparkSubmitOperator to clean, join, and load IMF data",
    tags=["spark", "etl", "sqlite", "postgres"],
) as dag:

    transform_task = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/airflow/scripts/transform.py",
        conn_id="spark_default",  # set this in Airflow Connections
        verbose=True,
        application_args=[],
    )

    join_and_load_task = SparkSubmitOperator(
        task_id="join_and_load_data",
        application="/opt/airflow/scripts/join_and_load.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[],
    )

    transform_task >> join_and_load_task
