from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from datetime import datetime, timedelta
import json
import sys

# Add the path to your functions file (adjust if needed)
sys.path.append('/opt/airflow/fpl_functions')
from Functions import (
    get_players, get_teams, get_fixtures, get_events, 
    get_event_live 
)

# --- Airflow DAG Configuration ---

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 21),  # Update as needed
    'email': ['your_email@example.com'],  # Update with your email
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fpl_data_pipeline',  
    default_args=default_args,
    description='1-FPL Data Pipeline with Kafka and Spark',
    schedule_interval=timedelta(days=1),  # Run daily, adjust as needed
    catchup=False
)

# --- Kafka Producer Function --- 
def producer_function(messages):
    """Prepares messages for Kafka by serializing them to JSON format and encoding to bytes."""
    return [json.dumps(message).encode('utf-8') for message in messages]

# --- Data Fetching Tasks ---

def fetch_players():
    return get_players()

def fetch_teams():
    return get_teams()

def fetch_fixtures():
    return get_fixtures()

def fetch_events():
    return get_events()

def fetch_live_event_data():
    events = get_events()
    current_gameweek = next((event['id'] for event in events if event['is_current']), None)
    if current_gameweek:
        return get_event_live(current_gameweek)
    return None 

# --- PythonOperator Tasks for Fetching Data ---

fetch_players_task = PythonOperator(
    task_id='fetch_players',
    python_callable=fetch_players,
    dag=dag
)

fetch_teams_task = PythonOperator(
    task_id='fetch_teams',
    python_callable=fetch_teams,
    dag=dag
)

fetch_fixtures_task = PythonOperator(
    task_id='fetch_fixtures',
    python_callable=fetch_fixtures,
    dag=dag
)

fetch_events_task = PythonOperator(
    task_id='fetch_events',
    python_callable=fetch_events,
    dag=dag
)

fetch_live_event_data_task = PythonOperator(
    task_id='fetch_live_event_data',
    python_callable=fetch_live_event_data,
    dag=dag
)

# --- ProduceToTopicOperator Tasks for Sending to Kafka ---

produce_players_task = ProduceToTopicOperator(
    task_id='produce_players_to_kafka',
    topic='fpl_player_data',
    kafka_conn_id='kafka_default',  # Make sure this matches the connection ID in Airflow UI
    producer_function=producer_function,  # Use the correct producer function here
    data="{{ ti.xcom_pull(task_ids='fetch_players') }}",  # Pull the data from the XCom
    dag=dag
)

produce_teams_task = ProduceToTopicOperator(
    task_id='produce_teams_to_kafka',
    topic='fpl_team_data',
    kafka_conn_id='kafka_default',
    producer_function=producer_function,
    data="{{ ti.xcom_pull(task_ids='fetch_teams') }}",
    dag=dag
)

produce_fixtures_task = ProduceToTopicOperator(
    task_id='produce_fixtures_to_kafka',
    topic='fpl_fixture_data',
    kafka_conn_id='kafka_default',
    producer_function=producer_function,
    data="{{ ti.xcom_pull(task_ids='fetch_fixtures') }}",
    dag=dag
)

produce_events_task = ProduceToTopicOperator(
    task_id='produce_events_to_kafka',
    topic='fpl_event_data',
    kafka_conn_id='kafka_default',
    producer_function=producer_function,
    data="{{ ti.xcom_pull(task_ids='fetch_events') }}",
    dag=dag
)

produce_live_event_data_task = ProduceToTopicOperator(
    task_id='produce_live_event_data_to_kafka',
    topic='fpl_live_event_data',
    kafka_conn_id='kafka_default',
    producer_function=producer_function,
    data="{{ ti.xcom_pull(task_ids='fetch_live_event_data') }}",
    dag=dag
)

# --- Task Dependencies ---

fetch_players_task >> produce_players_task
fetch_teams_task >> produce_teams_task
fetch_fixtures_task >> produce_fixtures_task
fetch_events_task >> produce_events_task
fetch_live_event_data_task >> produce_live_event_data_task
