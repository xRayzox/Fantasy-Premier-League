from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_test',
    default_args=default_args,
    description='A simple DAG to test Spark job submission',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

spark_job = SparkSubmitOperator(
    task_id='spark_test_task',
    application='/opt/airflow/fpl_functions/spark_script.py',  # Path to your Spark job
    conn_id='spark_con',  # Ensure this connection ID is configured in Airflow
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',  # Kafka package for Spark
    verbose=True,
    dag=dag,
)
spark_job
