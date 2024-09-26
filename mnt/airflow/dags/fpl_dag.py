from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
import sys
import pandas as pd
import boto3

# Ensure the correct path is appended for importing custom functions
sys.path.append('/opt/airflow/fpl_functions')

# Import functions from the external file Functions.py
from Functions import load_fixture, load_fpl, load_player_history

# MinIO configuration
MINIO_HOST = 'minio:9000'
MINIO_ACCESS_KEY = 'JgaaxXsjCxGVJBbNKmup'
MINIO_SECRET_KEY = 'sQWHycmBpXlcoxDKluOMuc66LKJ756UqBDa7ofE7'
BUCKET_NAME = 'fpl'

# Initialize MinIO client
minio_client = boto3.client(
    's3',
    endpoint_url=f'http://{MINIO_HOST}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

default_args = {
    'owner': 'wael',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'fpl_data_pipeline',
    default_args=default_args,
    description='A simple Fantasy Premier League data pipeline',
    schedule_interval='@daily',  # Adjust as needed
)

######################################################
# Define Airflow tasks
######################################################
create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',  # Your Airflow connection ID
    sql="""
    CREATE TABLE IF NOT EXISTS gameweeks (
        id VARCHAR(255) PRIMARY KEY,
        GW_name VARCHAR(255),
        deadline_time TIMESTAMP,
        highest_score INTEGER,
        average_entry_score INTEGER,
        most_selected VARCHAR(255),
        most_transferred_in VARCHAR(255),
        top_element VARCHAR(255),
        most_captained VARCHAR(255),
        most_vice_captained VARCHAR(255)
    );

    CREATE TABLE IF NOT EXISTS positions (
        id VARCHAR(255) PRIMARY KEY,
        plural_name VARCHAR(255),
        plural_name_short VARCHAR(255),
        singular_name VARCHAR(255),
        singular_name_short VARCHAR(255)
    );

    CREATE TABLE IF NOT EXISTS teams (
        code VARCHAR(255),
        id VARCHAR(255),
        team_name VARCHAR(255),
        short_name VARCHAR(255),
        strength INTEGER,
        strength_overall_away INTEGER,
        strength_overall_home INTEGER,
        strength_attack_away INTEGER,
        strength_attack_home INTEGER,
        strength_defence_away INTEGER,
        strength_defence_home INTEGER,
        PRIMARY KEY(id),
        UNIQUE(code)
    );

    CREATE TABLE IF NOT EXISTS players (
        code VARCHAR(255),
        id VARCHAR(255),
        full_name VARCHAR(255),
        web_name VARCHAR(255),
        element_type VARCHAR(255),
        team VARCHAR(255),
        team_code VARCHAR(255),
        dreamteam_count INTEGER,
        news TEXT,
        value_season FLOAT,
        PRIMARY KEY(id),
        UNIQUE(code),
        FOREIGN KEY(element_type) REFERENCES positions(id) ON DELETE CASCADE,
        FOREIGN KEY(team) REFERENCES teams(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS player_history (
        season_name VARCHAR(255),
        element_code VARCHAR(255),
        start_cost FLOAT,
        end_cost FLOAT,
        total_points INT,
        "minutes" INT,
        goals_scored INT,
        assists INT,
        clean_sheets INT,
        goals_conceded INT,
        own_goals INT,
        penalties_saved INT,
        penalties_missed INT,
        yellow_cards INT,
        red_cards INT,
        saves INT,
        bonus INT,
        PRIMARY KEY (season_name, element_code),
        CONSTRAINT fk_players FOREIGN KEY(element_code)
        REFERENCES players(code) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS fact_table (
        element VARCHAR(255),
        fixture VARCHAR(255),
        opponent_team VARCHAR(255),
        total_points INTEGER,
        was_home BOOLEAN,
        kickoff_time TIMESTAMP,
        team_h_score INTEGER,
        team_a_score INTEGER,
        GW VARCHAR(255),
        minutes_played INTEGER,
        goals_scored INTEGER,
        assists INTEGER,
        clean_sheets INTEGER,
        goals_conceded INTEGER,
        own_goals INTEGER,
        penalties_saved INTEGER,
        penalties_missed INTEGER,
        yellow_cards INTEGER,
        red_cards INTEGER,
        saves INTEGER,
        bonus INTEGER,
        bps FLOAT,
        influence FLOAT,
        creativity FLOAT,
        threat FLOAT,
        ict_index FLOAT,
        starts INTEGER,
        expected_goals VARCHAR(255),
        expected_assists VARCHAR(255),
        expected_goal_involvements VARCHAR(255),
        expected_goals_conceded VARCHAR(255),
        price FLOAT,
        transfers_balance INTEGER,
        selected INTEGER,
        transfers_in INTEGER,
        transfers_out INTEGER,
        PRIMARY KEY (element, fixture, GW),
        FOREIGN KEY (element) REFERENCES players(id) ON DELETE CASCADE,
        FOREIGN KEY (opponent_team) REFERENCES teams(id) ON DELETE CASCADE,
        FOREIGN KEY (GW) REFERENCES gameweeks(id) ON DELETE CASCADE
    );
    """,
    dag=dag,
)

# Task to load FPL static data and push it to XCom for other tasks
def load_fpl_task_callable(**kwargs):
    players_df, teams_df, positions_of_players_df, gameweeks_df = load_fpl()
    kwargs['ti'].xcom_push(key='players_df', value=players_df)
    players_path = '/opt/airflow/fpl_functions/fpl_extract/players.csv'
    teams_path = '/opt/airflow/fpl_functions/fpl_extract/teams.csv'
    positions_of_players_path = '/opt/airflow/fpl_functions/fpl_extract/positions_of_players.csv'
    gameweeks_path = '/opt/airflow/fpl_functions/fpl_extract/gameweeks.csv'
    players_df.to_csv(players_path, index=False)
    teams_df.to_csv(teams_path, index=False)
    positions_of_players_df.to_csv(positions_of_players_path, index=False)
    gameweeks_df.to_csv(gameweeks_path, index=False)
    # Upload CSVs to MinIO
    minio_client.upload_file(players_path, BUCKET_NAME, 'data/players.csv')
    minio_client.upload_file(teams_path, BUCKET_NAME, 'data/teams.csv')
    minio_client.upload_file(positions_of_players_path, BUCKET_NAME, 'data/positions.csv')
    minio_client.upload_file(gameweeks_path, BUCKET_NAME, 'data/gameweeks.csv')

    

load_fpl_task = PythonOperator(
    task_id='load_fpl',
    python_callable=load_fpl_task_callable,
    provide_context=True,
    dag=dag,
)

# Task to load player history using the players_df from load_fpl_task
def players_statistics(**kwargs):
    # Retrieve players_df from XCom
    ti = kwargs['ti']
    players_df = ti.xcom_pull(task_ids='load_fpl', key='players_df')
    # Call load_player_history with the retrieved df
    current_season_df, previous_seasons_df = load_player_history(players_df)
    
    # Save current_season_df to CSV
    current_season_path = '/opt/airflow/fpl_functions/fpl_extract/current_season.csv'
    previous_seasons_path = '/opt/airflow/fpl_functions/fpl_extract/previous_seasons.csv'

    current_season_df.to_csv(current_season_path, index=False)
    previous_seasons_df.to_csv(previous_seasons_path, index=False)
    

    # Upload CSVs to MinIO
    minio_client.upload_file(current_season_path, BUCKET_NAME, 'data/current_season.csv')
    minio_client.upload_file(previous_seasons_path, BUCKET_NAME, 'data/previous_seasons.csv')
    

    return current_season_df, previous_seasons_df

load_players_statistics = PythonOperator(
    task_id='load_players_statistics',
    python_callable=players_statistics,
    provide_context=True,
    dag=dag,
)

# Task to transform data
spark_transform_current_task = SparkSubmitOperator(
    task_id='transform_fpl_data',
    application='/opt/airflow/fpl_functions/tr_current_season_fact_table.py',
    packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.4',
    conn_id='spark_default',  # Spark connection in Airflow
    dag=dag,
)
spark_transform_previous_task = SparkSubmitOperator(
    task_id='transform_previous_season_data',
    application='/opt/airflow/fpl_functions/tr_history_season.py',  # Path to your previous season script
    packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.4',
    conn_id='spark_default',  # Spark connection in Airflow
    dag=dag,
)
spark_transform_gameweeks_task = SparkSubmitOperator(
    task_id='transform_gameweeks_data',
    application='/opt/airflow/fpl_functions/tr_gameweeks_data.py',  # Path to the script
    packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.4',
    conn_id='spark_default',  # Spark connection in Airflow
    dag=dag,
)
spark_transform_position_task = SparkSubmitOperator(
    task_id='transform_positions_data',
    application='/opt/airflow/fpl_functions/tr_position.py',  # Path to the position transformation script
    packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.4',
    conn_id='spark_default',  # Spark connection in Airflow
    dag=dag,
)
spark_transform_teams_task = SparkSubmitOperator(
    task_id='transform_teams_data',
    application='/opt/airflow/fpl_functions/tr_teams.py',  # Path to the team transformation script
    packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.4',
    conn_id='spark_default',  # Spark connection in Airflow
    dag=dag,
)
spark_transform_players_task = SparkSubmitOperator(
    task_id='transform_players_data',
    application='/opt/airflow/fpl_functions/tr_players.py',  # Path to the player transformation script
    packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.4',
    conn_id='spark_default',  # Spark connection in Airflow
    dag=dag,
)
######################################################
# Task dependencies
######################################################

# Define task dependencies
create_tables_task >> load_fpl_task
load_fpl_task >> [load_players_statistics,spark_transform_players_task,spark_transform_gameweeks_task,spark_transform_position_task,spark_transform_teams_task] 
load_players_statistics >> [spark_transform_current_task, spark_transform_previous_task]

