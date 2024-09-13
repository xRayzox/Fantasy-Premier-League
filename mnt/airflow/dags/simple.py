from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json
import time
import random

# Set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'kafka_producer_dag',
    default_args=default_args,
    description='A simple DAG to produce data to Kafka',
    schedule_interval=None,  # Set your schedule interval or use cron expressions
    start_date=datetime(2024, 9, 13),
    catchup=False,
)

# Define the function to run
def produce_data_to_kafka():
    def generate_data():
        return {'value': random.randint(1, 100)}

    kafka_topic = "example_data"
    kafka_bootstrap_servers = "kafka:9092"

    # Producer configuration
    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
    }

    # Create a Producer instance
    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    for i in range(8):
        data = generate_data()
        producer.produce(kafka_topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.poll(1)  # Wait up to 1 second for events to be processed
        time.sleep(1)
    
    # Ensure all messages are delivered before exiting
    producer.flush()

# Create a PythonOperator to run the function
produce_data_task = PythonOperator(
    task_id='produce_data_task',
    python_callable=produce_data_to_kafka,
    dag=dag,
)

# Set task dependencies if needed
produce_data_task
