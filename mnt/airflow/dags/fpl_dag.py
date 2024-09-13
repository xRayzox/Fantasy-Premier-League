from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json
import time
import sys
# Add the path to your functions file (adjust if needed)
sys.path.append('/opt/airflow/fpl_functions')
from Functions import (
    get_players, get_teams, get_fixtures, get_events, 
    get_event_live 
)

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
    'fpl_data_pipeline',
    default_args=default_args,
    description='FPL Data Pipeline with Confluent Kafka',
    schedule_interval=timedelta(days=1),  # Adjust as needed
    start_date=datetime(2023, 12, 21),
    catchup=False,
)

# Define the function to run
def produce_data_to_kafka():
    kafka_topic_prefix = "FPL_"  # Prefix for different topics
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

    # Dictionary of functions and their respective Kafka topics
    functions_and_topics = {
        'players': get_players,
        'teams': get_teams,
        'fixtures': get_fixtures,
        'events': get_events,
        'event_live': get_event_live
    }

    # Iterate over the functions and topics
    for key, func in functions_and_topics.items():
        data = func()
        kafka_topic = f"{kafka_topic_prefix}{key.capitalize()}"
        for item in data:
            # Use the 'id' field as the key (adjust if necessary)
            key = str(item.get('id', 'default_key'))
            value = json.dumps(item).encode('utf-8')
            producer.produce(kafka_topic, key=key, value=value, callback=delivery_report)
            producer.poll(1)  # Wait up to 1 second for events to be processed
            time.sleep(1)

    # Ensure all messages are delivered before exiting
    producer.flush()



# Define PythonOperator task for fetching teams data and producing to Kafka
produce_data_task = PythonOperator(
    task_id='produce_data_task',
    python_callable=produce_data_to_kafka,
    dag=dag,
)

# Set task dependencies if needed
produce_data_task

