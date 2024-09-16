from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sys

sys.path.append('/opt/airflow/fpl_functions')
from Functions import get_fpl_data, get_fixtures_data, get_players_history

def fetch_fpl_data(**kwargs):
    """Fetches FPL data from the API and pushes it to XCom."""
    data = get_fpl_data()
    # Push the entire data to XCom
    kwargs['ti'].xcom_push(key='fpl_data', value=data)
    # Push player IDs to XCom for use in player history extraction
    player_ids = [player['id'] for player in data['elements']]
    kwargs['ti'].xcom_push(key='player_ids', value=player_ids)
    return data

def extract_and_load_players(**kwargs):
    """Extracts player data from XCom."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_fpl_data', key='fpl_data')
    players_df = pd.DataFrame(data['elements'])  # Player data
    return players_df

def extract_and_load_teams(**kwargs):
    """Extracts team data from XCom and pushes it as JSON to XCom."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_fpl_data', key='fpl_data')
    teams_df = pd.DataFrame(data['teams'])
    
    # Convert DataFrame to JSON string
    teams_json = teams_df.to_json(orient='records')
    
    # Push JSON string to XCom
    ti.xcom_push(key='teams_data_json', value=teams_json)
    return teams_df

def extract_and_load_events(**kwargs):
    """Extracts gameweek data from XCom and pushes it as JSON to XCom."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_fpl_data', key='fpl_data')
    events_df = pd.DataFrame(data['events'])  # Gameweek data
    
    # Convert DataFrame to JSON string
    events_json = events_df.to_json(orient='records')
    
    # Push JSON string to XCom
    ti.xcom_push(key='events_data_json', value=events_json)
    return events_df

def extract_and_load_fixtures(**kwargs):
    """Extracts fixtures data from the API."""
    fixtures_data = get_fixtures_data()
    fixtures_df = pd.DataFrame(fixtures_data)
    return fixtures_df

def extract_and_load_player_history(batch_size=50, **kwargs):
    """Extracts player history data in chunks, using player IDs from XCom."""
    ti = kwargs['ti']
    player_ids = ti.xcom_pull(task_ids='fetch_fpl_data', key='player_ids')
    all_player_history = []

    for i in range(0, len(player_ids), batch_size):
        batch_ids = player_ids[i:i + batch_size]
        player_history = get_players_history(batch_ids)
        all_player_history.extend(player_history)

    history_df = pd.DataFrame(all_player_history)
    return history_df

# Default args for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the Airflow DAG
with DAG(
    dag_id='fpl_data_pipeline',
    default_args=default_args,
    description='A DAG to extract and transform FPL data',
    schedule_interval='@daily',  # Adjust frequency as needed
    catchup=False,
) as dag:

    # Task 1: Fetch FPL data
    fetch_fpl_data_task = PythonOperator(
        task_id='fetch_fpl_data',
        python_callable=fetch_fpl_data,
        provide_context=True
    )

    # Task 2: Extract player data
    extract_load_players_task = PythonOperator(
        task_id='extract_and_load_players',
        python_callable=extract_and_load_players,
        provide_context=True,
    )

    # Task 3: Extract team data
    extract_load_teams_task = PythonOperator(
        task_id='extract_and_load_teams',
        python_callable=extract_and_load_teams,
        provide_context=True,
    )

    # Task 4: Extract events data
    extract_load_events_task = PythonOperator(
        task_id='extract_and_load_events',
        python_callable=extract_and_load_events,
        provide_context=True,
    )

    # Task 5: Extract fixtures data
    extract_load_fixtures_task = PythonOperator(
        task_id='extract_and_load_fixtures',
        python_callable=extract_and_load_fixtures,
        provide_context=True,
    )

    # Task 6: Extract player history data
    extract_load_player_history_task = PythonOperator(
        task_id='extract_and_load_player_history',
        python_callable=extract_and_load_player_history,
        provide_context=True,
    )

    # Task 7: Transform teams data using Spark
    transform_teams_task = SparkSubmitOperator(
        task_id='transform_teams_data',
        application='/opt/airflow/fpl_functions/teams_script.py',  # Path to your Spark script
        conn_id='spark_conn',  # Airflow Spark connection ID
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0',
        application_args=["{{ ti.xcom_pull(task_ids='extract_and_load_teams', key='teams_data_json') }}"],  # Pass JSON as argument
        dag=dag
    )

    # Task 7: Transform events data using Spark
    transform_events_task = SparkSubmitOperator(
        task_id='transform_events_data',
        application='/opt/airflow/fpl_functions/events_script.py',  # Path to your Spark events script
        conn_id='spark_conn',  # Airflow Spark connection ID
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0',
        application_args=["{{ ti.xcom_pull(task_ids='extract_and_load_events', key='events_data_json') }}"],  # Pass JSON string from XCom
        dag=dag
    )
    # Define the order of tasks
    fetch_fpl_data_task >> [extract_load_players_task, extract_load_teams_task, extract_load_events_task, extract_load_player_history_task]
    extract_load_fixtures_task >> transform_teams_task
    extract_load_fixtures_task >> transform_events_task
