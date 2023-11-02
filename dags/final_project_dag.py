"""
DAG ini digunakan untuk menjalankan script ETL.py dan AnalisisSpotify.py
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

# Parameter DAG
spark_dag = DAG(
    dag_id="Spotify_Music_Listener_Behaviour",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Extract data dari CSV, tranformasi, dan hasilnya di load ke postgresql",
    start_date=days_ago(1),
)

# Task untuk menjalankan spark-scripts ETL.py
etldata_task = SparkSubmitOperator(
    application="/spark-scripts/ETL.py",
    conn_id="spark_tgs",
    task_id="ETLData",
    jars="/jar/postgresql-42.2.18.jar",
    application_args="/spark-scripts/ETL.py",
    dag=spark_dag,
)

# Task untuk menjalankan spark-scripts AnalisisSpotify.py
analisis_data_task = SparkSubmitOperator(
    application="/spark-scripts/AnalisisSpotify.py",
    conn_id="spark_tgs",
    task_id="AnalisisData",
    jars="/jar/postgresql-42.2.18.jar",
    application_args="/spark-scripts/AnalisisSpotify.py",
    dag=spark_dag,
)

# Ketergantungan atau urutan task
etldata_task >> analisis_data_task
