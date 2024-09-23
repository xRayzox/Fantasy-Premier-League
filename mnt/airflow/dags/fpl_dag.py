from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
import sys
import pandas as pd

# Ensure the correct path is appended for importing custom functions
sys.path.append('/opt/airflow/fpl_functions')

# Import functions from the external file Functions.py
from Functions import load_fixture, load_fpl, load_player_history

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

# Task to load fixture data
load_fixture_task = PythonOperator(
    task_id='load_fixture',
    python_callable=load_fixture,
    dag=dag,
)

# Task to load FPL static data and push it to XCom for other tasks
def load_fpl_task_callable(**kwargs):
    players_df, teams_df, positions_of_players_df, gameweeks_df = load_fpl()
    # Push players_df to XCom so it can be used in the next task
    kwargs['ti'].xcom_push(key='players_df', value=players_df)

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
    current_season_path = '/opt/airflow/fpl_functions/current_season.csv'
    current_season_df.to_csv(current_season_path, index=False)
    
    # Save previous_seasons_df to CSV
    previous_seasons_path = '/opt/airflow/fpl_functions/previous_seasons.csv'
    previous_seasons_df.to_csv(previous_seasons_path, index=False)
    
    # Push paths to XCom
    kwargs['ti'].xcom_push(key='current_season_csv', value=current_season_path)
    kwargs['ti'].xcom_push(key='previous_seasons_csv', value=previous_seasons_path)
    
    return current_season_df, previous_seasons_df

load_players_statistics = PythonOperator(
    task_id='load_players_statistics',
    python_callable=players_statistics,
    provide_context=True,
    dag=dag,
)

# Task to transform data
spark_transform_task = SparkSubmitOperator(
    task_id='transform_fpl_data',
    application='/opt/airflow/fpl_functions/tr_current_season_fact_table.py',  # Path to your PySpark script
    conn_id='spark_default',  # Spark connection in Airflow
    application_args=['{{ ti.xcom_pull(task_ids="load_players_statistics", key="current_season_csv") }}'],
    dag=dag,
)
# Task to insert data into PostgreSQL
insert_fact_table_task = PostgresOperator(
    task_id='insert_fact_table',
    postgres_conn_id='postgres_default',  # Your Airflow connection ID
    sql="""
        INSERT INTO fact_table (element, fixture, opponent_team, total_points, was_home, kickoff_time, team_h_score,
        team_a_score, GW, minutes_played, goals_scored, assists, clean_sheets, goals_conceded, own_goals,
        penalties_saved, penalties_missed, yellow_cards, red_cards, saves, bonus, bps, influence, creativity,
        threat, ict_index, starts, expected_goals, expected_assists, expected_goal_involvements, expected_goals_conceded,
        price, transfers_balance, selected, transfers_in, transfers_out)
        SELECT * FROM (VALUES 
        {{ ti.xcom_pull(task_ids='transform_fpl_data') | json_query }} 
        ) AS temp (element, fixture, opponent_team, total_points, was_home, kickoff_time, team_h_score,
        team_a_score, GW, minutes_played, goals_scored, assists, clean_sheets, goals_conceded, own_goals,
        penalties_saved, penalties_missed, yellow_cards, red_cards, saves, bonus, bps, influence, creativity,
        threat, ict_index, starts, expected_goals, expected_assists, expected_goal_involvements, expected_goals_conceded,
        price, transfers_balance, selected, transfers_in, transfers_out)
    """,
    dag=dag,
)

######################################################
# Task dependencies
######################################################

# Define task dependencies
create_tables_task
load_fpl_task >> load_players_statistics >> spark_transform_task >>insert_fact_table_task
load_fixture_task
