from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sys
from datetime import timedelta
import pandas as pd
sys.path.append('/opt/airflow/fpl_functions')
from Functions import get_bootstrap_static_data, get_fixtures_data, get_player_history_data, FPLElementType, FPLElement, FPLTeam, FPLEvent, FPLFixture, FPLHistory

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fpl_dag',
    default_args=default_args,
    description='DAG for fetching and processing FPL data',
    schedule_interval='@daily',  # Adjust as needed
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def fetch_bootstrap_data(**kwargs):
        data = get_bootstrap_static_data()
        kwargs['ti'].xcom_push(key='bootstrap_data', value=data)

    def process_element_types(**kwargs):
        bootstrap_data = kwargs['ti'].xcom_pull(task_ids='fetch_bootstrap_data', key='bootstrap_data')
        element_types_df = pd.concat([FPLElementType(item).to_dataframe() for item in bootstrap_data['element_types']], ignore_index=True)
        kwargs['ti'].xcom_push(key='element_types_df', value=element_types_df.to_dict())

    def process_elements(**kwargs):
        bootstrap_data = kwargs['ti'].xcom_pull(task_ids='fetch_bootstrap_data', key='bootstrap_data')
        elements_df = pd.concat([FPLElement(item).to_dataframe() for item in bootstrap_data['elements']], ignore_index=True)
        kwargs['ti'].xcom_push(key='elements_df', value=elements_df.to_dict())

    def process_teams(**kwargs):
        bootstrap_data = kwargs['ti'].xcom_pull(task_ids='fetch_bootstrap_data', key='bootstrap_data')
        teams_df = pd.concat([FPLTeam(item).to_dataframe() for item in bootstrap_data['teams']], ignore_index=True)
        kwargs['ti'].xcom_push(key='teams_df', value=teams_df.to_dict())

    def process_events(**kwargs):
        bootstrap_data = kwargs['ti'].xcom_pull(task_ids='fetch_bootstrap_data', key='bootstrap_data')
        events_df = pd.concat([FPLEvent(item).to_dataframe() for item in bootstrap_data['events']], ignore_index=True)
        kwargs['ti'].xcom_push(key='events_df', value=events_df.to_dict())

    def fetch_fixtures_data(**kwargs):
        data = get_fixtures_data()
        kwargs['ti'].xcom_push(key='fixtures_data', value=data)

    def process_fixtures(**kwargs):
        fixtures_data = kwargs['ti'].xcom_pull(task_ids='fetch_fixtures_data', key='fixtures_data')
        fixtures_df = pd.concat([FPLFixture(item).to_dataframe() for item in fixtures_data], ignore_index=True)
        kwargs['ti'].xcom_push(key='fixtures_df', value=fixtures_df.to_dict())

    def process_player_history(**kwargs):
        elements_df = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='process_elements', key='elements_df'))
        player_ids = elements_df['id'].tolist()
        all_player_history = []
        for player_id in player_ids:
            player_history = get_player_history_data(player_id)
            all_player_history.extend([FPLHistory(item).to_dataframe() for item in player_history])
        history_df = pd.concat(all_player_history, ignore_index=True)
        kwargs['ti'].xcom_push(key='history_df', value=history_df.to_dict())

    def save_to_csv(**kwargs):
        for key in ['element_types_df', 'elements_df', 'teams_df', 'events_df', 'fixtures_df', 'history_df']:
            df = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='process_player_history', key=key))
            df.to_csv(f"/opt/airflow/dags/{key}.csv", index=False)

    fetch_bootstrap = PythonOperator(
        task_id='fetch_bootstrap_data',
        python_callable=fetch_bootstrap_data,
        provide_context=True
    )

    process_element_types_task = PythonOperator(
        task_id='process_element_types',
        python_callable=process_element_types,
        provide_context=True
    )

    process_elements_task = PythonOperator(
        task_id='process_elements',
        python_callable=process_elements,
        provide_context=True
    )

    process_teams_task = PythonOperator(
        task_id='process_teams',
        python_callable=process_teams,
        provide_context=True
    )

    process_events_task = PythonOperator(
        task_id='process_events',
        python_callable=process_events,
        provide_context=True
    )

    fetch_fixtures = PythonOperator(
        task_id='fetch_fixtures_data',
        python_callable=fetch_fixtures_data,
        provide_context=True
    )

    process_fixtures_task = PythonOperator(
        task_id='process_fixtures',
        python_callable=process_fixtures,
        provide_context=True
    )

    process_player_history_task = PythonOperator(
        task_id='process_player_history',
        python_callable=process_player_history,
        provide_context=True
    )

    save_csv = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True
    )

    # Set task dependencies
    fetch_bootstrap >> [process_element_types_task, process_elements_task, process_teams_task, process_events_task]
    fetch_fixtures >> process_fixtures_task
    process_elements_task >> process_player_history_task
    process_fixtures_task >> process_player_history_task
    process_player_history_task >> save_csv
