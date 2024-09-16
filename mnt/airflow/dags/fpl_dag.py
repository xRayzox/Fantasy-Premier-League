from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json
import sys
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Add the path to your functions file (adjust if needed)
sys.path.append('/opt/airflow/fpl_functions')
from Functions import get_fpl_data, get_fixtures_data, get_players_history

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
    '12-fpl_data_pipeline_with_kafka',
    default_args=default_args,
    description='FPL Data Pipeline with Confluent Kafka',
    schedule_interval=timedelta(days=1),  # Adjust as needed
    start_date=datetime(2023, 12, 21),
    catchup=False,
)

# Kafka producer creation
def create_producer():
    kafka_bootstrap_servers = "kafka:9092"
    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'enable.idempotence': True,
    }
    return Producer(conf)

# Function to produce data to Kafka topic
def produce_to_kafka(topic, data, key_id):
    producer = create_producer()
    
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    seen_keys = set()
    for item in data:
        key = str(item.get(key_id, ''))
        if key not in seen_keys:
            seen_keys.add(key)
            value = json.dumps(item).encode('utf-8')
            producer.produce(topic, key=key, value=value, callback=delivery_report)
            producer.poll(1)

    producer.flush()

# Task 1: Fetch FPL data (players, teams, gameweeks)
def fetch_fpl_data():
    try:
        # Fetch data from the FPL API
        data = get_fpl_data()
        
        # Extract relevant sections
        players_data = data['elements']
        teams_data = data['teams']
        gameweeks_data = data['events']
        
        # Prepare the data to be converted to JSON strings
        return {
            'players_data': players_data,
            'teams_data': teams_data,
            'gameweeks_data': gameweeks_data
        }
    except Exception as e:
        print(f"Error fetching FPL data: {e}")
        return {}

# Task 2: Fetch and produce teams
def fetch_and_produce_teams(**kwargs):
    teams_data = get_fpl_data()['teams']
    produce_to_kafka("FPL_Teams", teams_data, "id")
    return json.dumps(teams_data, indent=4)

# Task 3: Fetch and produce players
def fetch_and_produce_players(**kwargs):
    players_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_fpl_data')['players_data']
    produce_to_kafka("FPL_Players", players_data, "id")

# Task 4: Fetch and produce gameweeks
def fetch_and_produce_gameweeks(**kwargs):
    gameweeks_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_fpl_data')['gameweeks_data']
    produce_to_kafka("FPL_Gameweeks", gameweeks_data, "id")

# Task 5: Fetch and produce fixtures data
def fetch_and_produce_fixtures():
    fixtures_data = get_fixtures_data()
    produce_to_kafka("FPL_Fixtures", fixtures_data, "id")

# Task 6: Fetch player history data in parallel and produce it
def fetch_and_produce_player_history(**kwargs):
    # Fetch only player IDs from XCom
    players_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_fpl_data')['players_data']
    player_ids = [player['id'] for player in players_data]
    player_history = get_players_history(player_ids)
    produce_to_kafka("FPL_PlayerHistory", player_history, "element")

# Define PythonOperator tasks
fetch_fpl_data_task = PythonOperator(
    task_id='fetch_fpl_data',
    python_callable=fetch_fpl_data,
    dag=dag,
)

fetch_and_produce_teams_task = PythonOperator(
    task_id='fetch_and_produce_teams',
    python_callable=fetch_and_produce_teams,
    provide_context=True,
    dag=dag,
)

fetch_and_produce_players_task = PythonOperator(
    task_id='fetch_and_produce_players',
    python_callable=fetch_and_produce_players,
    provide_context=True,
    dag=dag,
)

fetch_and_produce_gameweeks_task = PythonOperator(
    task_id='fetch_and_produce_gameweeks',
    python_callable=fetch_and_produce_gameweeks,
    provide_context=True,
    dag=dag,
)

fetch_and_produce_fixtures_task = PythonOperator(
    task_id='fetch_and_produce_fixtures',
    python_callable=fetch_and_produce_fixtures,
    dag=dag,
)

fetch_and_produce_player_history_task = PythonOperator(
    task_id='fetch_and_produce_player_history',
    python_callable=fetch_and_produce_player_history,
    provide_context=True,
    dag=dag,
)

spark_job = SparkSubmitOperator(
    task_id='spark_test_task',
    application='/opt/airflow/fpl_functions/spark_script.py',  # Path to your Spark job
    conn_id='spark_con',  # Ensure this connection ID is configured in Airflow
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0',  # Kafka package for Spark
    application_args=["{{ti.xcom_pull(task_ids='fetch_and_produce_teams')}}"],  # Pass teams_data in JSON format
    verbose=True,
    dag=dag,
)

# Set task dependencies
fetch_fpl_data_task >> [
    fetch_and_produce_teams_task,
    fetch_and_produce_players_task,
    fetch_and_produce_gameweeks_task,
    fetch_and_produce_fixtures_task
] >> fetch_and_produce_player_history_task

fetch_and_produce_teams_task >> spark_job
