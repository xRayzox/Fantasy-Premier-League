from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '13-spark_test',
    default_args=default_args,
    description='A simple DAG to test Spark job submission',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

def pull_function(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='spark_test_task')
    print(ls) # Here teams_data should be in a format you expect; otherwise, adjust as needed

spark_job = SparkSubmitOperator(
    task_id='spark_test_task',
    application='/opt/airflow/fpl_functions/spark_script.py',  # Path to your Spark job
    conn_id='spark_con',  # Ensure this connection ID is configured in Airflow
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0',  # Kafka package for Spark
    application_args=["{{ti.xcom_pull(task_ids='run_task_1')}}"],
    verbose=True,
    dag=dag,
)

fetch = PythonOperator(
    task_id='fetch_fpl_data',
    python_callable=pull_function,
    provide_context=True,
    dag=dag,
)

spark_job >> fetch
