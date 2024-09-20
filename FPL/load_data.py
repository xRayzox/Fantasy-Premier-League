import configparser
import pathlib
import psycopg2
import sys
from psycopg2 import sql
from validation import validate_input

"""
Part of DAG. Upload CSV files to PostgreSQL. Takes one argument of format YYYYMMDD. Script will load data into temporary table in PostgreSQL, delete 
records with the same ID from main table, then insert these from temp table (along with new data) 
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in PostgreSQL will be updated to reflect any changes in that record, if any
"""

#Read Configuration File
parser = configparser.ConfigParser()
config_file = "/opt/airflow/dbt/configuration.conf"
parser.read(config_file)

#Configuration Variables
DATABASE = parser.get("PostgreSQL","database")
SCHEMA = parser.get("PostgreSQL","schema")
USER = parser.get("PostgreSQL","user")
PASSWORD = parser.get("PostgreSQL","password")
HOST = 'postgres'
PORT = parser.get("PostgreSQL","port")
TEAMS_TABLE = "teams"
PLAYERS_TABLE = "players"
GAMEWEEK_TABLE = "gameweek"
FIXTURES_TABLE = "fixtures"
MY_TEAM_TABLE = "my_team"
MY_TEAM_TANSFERS_TABLE = "my_team_transfers"

# Check command line argument passed
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error {e}")
    sys.exit(1)

# Create team table if it doesn't exist
sql_create_teams_table = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id smallint PRIMARY KEY,
        name varchar,
        short_name varchar(3),
        strength smallint,
        strength_overall_home integer,
        strength_overall_away integer,
        strength_attack_home integer,
        strength_attack_away integer,
        strength_defence_home integer,
        strength_defence_away integer
    );
    """
).format(table=sql.Identifier(TEAMS_TABLE))

# Create players table if it doesn't exist
sql_create_players_table = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id smallint PRIMARY KEY,
        chance_of_playing_next_round float,
        chance_of_playing_this_round float,
        dreamteam_count smallint,
        ep_next float,
        ep_this float,
        event_points smallint,
        first_name varchar,
        form float,
        in_dreamteam boolean,
        news varchar,
        news_added timestamp with time zone,
        now_cost integer,
        points_per_game float,
        second_name varchar,
        selected_by_percent float,
        team smallint,
        total_points integer,
        transfers_in bigint,
        transfers_in_event bigint,
        transfers_out bigint,
        transfers_out_event bigint,
        value_form float,
        value_season float,
        web_name varchar,
        minutes integer,
        goals_scored smallint,
        assists smallint,
        clean_sheets smallint,
        goals_conceded integer,
        own_goals smallint,
        penalties_saved smallint,
        penalties_missed smallint,
        yellow_cards smallint,
        red_cards smallint,
        saves smallint,
        bonus integer,
        starts smallint,
        expected_goals float,
        expected_assists float,
        expected_goal_involvement float,
        expected_goals_conceded float,
        expected_goals_per_90 float,
        saves_per_90 float,
        expected_assists_per_90 float,
        expected_goal_involvements_per_90 float,
        expected_goals_conceded_per_90 float,
        goals_conceded_per_90 float,
        starts_per_90 float,
        clean_sheets_per_90 float,
        position varchar(3)
    );
    """
).format(table=sql.Identifier(PLAYERS_TABLE))

# Create Gameweek table if it doesn't exist
sql_create_gameweek_table = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id smallint PRIMARY KEY,
        name varchar,
        deadline_time timestamp with time zone,
        average_entry_score integer,
        finished boolean,
        data_checked boolean,
        highest_scoring_entry bigint,
        highest_score integer,
        is_previous boolean,
        is_current boolean,
        is_next boolean,
        most_selected integer,
        most_transferred_in integer,
        most_scoring_player integer,
        transfers_made bigint,
        most_captained integer,
        most_vice_captained integer,
        benchboost bigint,
        triple_captain bigint,
        wildcard bigint,
        freehit bigint,
        most_scoring_player_points smallint,  
        FOREIGN KEY (most_selected) REFERENCES {ref_table} (id),
        FOREIGN KEY (most_transferred_in) REFERENCES {ref_table} (id),
        FOREIGN KEY (most_scoring_player) REFERENCES {ref_table} (id),
        FOREIGN KEY (most_captained) REFERENCES {ref_table} (id),
        FOREIGN KEY (most_vice_captained) REFERENCES {ref_table} (id),
        FOREIGN KEY (most_scoring_player) REFERENCES {ref_table} (id)
    );
    """
).format(
    table=sql.Identifier(GAMEWEEK_TABLE),
    ref_table=sql.Identifier(PLAYERS_TABLE))

# Create Fixtures table if it doesn't exist
sql_create_fixtures_table = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id smallint PRIMARY KEY,
        gameweek smallint,
        finished boolean,
        kickoff_time timestamp with time zone,
        minutes smallint,
        started boolean,
        away_team smallint,
        away_team_score smallint,
        home_team smallint,
        home_team_score smallint,
        home_team_difficulty smallint,
        away_team_difficulty smallint,
        FOREIGN KEY (gameweek) REFERENCES {ref_table_1} (id),
        FOREIGN KEY (away_team) REFERENCES {ref_table_2} (id),
        FOREIGN KEY (home_team) REFERENCES {ref_table_2} (id)
    );
    """
).format(
    table=sql.Identifier(FIXTURES_TABLE),
    ref_table_1=sql.Identifier(GAMEWEEK_TABLE),
    ref_table_2=sql.Identifier(TEAMS_TABLE))

# Create My Team table if it doesn't exist
sql_create_myteam_table = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id smallint PRIMARY KEY,
        gameweek smallint,
        points integer,
        total_points integer,
        rank bigint,
        rank_sort bigint,
        overall_rank bigint,
        bank integer,
        value integer,
        gameweek_transfers smallint,
        gameweek_transfers_point_cost smallint,
        points_on_bench smallint,
        FOREIGN KEY (id) REFERENCES {ref_table} (id)
    );
    """
).format(
    table=sql.Identifier(MY_TEAM_TABLE),
    ref_table=sql.Identifier(GAMEWEEK_TABLE))

# Create My Team Transfers table if it doesn't exist
sql_create_myteamtransfers_table = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id integer PRIMARY KEY,
        player_in integer,
        player_in_cost integer,
        player_out integer,
        player_out_cost integer,
        gameweek smallint,
        time timestamp with time zone,
        FOREIGN KEY (player_in) REFERENCES {ref_table_1} (id),
        FOREIGN KEY (player_out) REFERENCES {ref_table_1} (id),
        FOREIGN KEY (gameweek) REFERENCES {ref_table_2} (id)
    );
    """
).format(
    table=sql.Identifier(MY_TEAM_TANSFERS_TABLE),
    ref_table_1=sql.Identifier(PLAYERS_TABLE),
    ref_table_2=sql.Identifier(GAMEWEEK_TABLE))

# UPSERT data by first loading into a temp table. Then, delete all records in main tables that match id with temp table. Finally, load data from temp table into main table
def create_temp_table(TABLE_NAME):

    output = sql.SQL(
        """
        CREATE TEMP TABLE our_staging_table (
            LIKE {table}
        );
        """
    ).format(table=sql.Identifier(TABLE_NAME))

    return output

def sql_copy_to_temp(file):
    tmp_dir = f"/opt/airflow/fpl_functions/tmp"
    file_path = f"{tmp_dir}/{file}_{output_name}.csv"

    copy_statement = f"COPY our_staging_table FROM STDIN DELIMITER ',' CSV HEADER;"

    file_object = open(file_path, encoding='utf-8')

    return copy_statement, file_object

def column_names_sql(TABLE_NAME):

    output = sql.SQL(
    """
    select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {table} and TABLE_SCHEMA = {schema}
    
    """
    ).format(table=sql.Literal(TABLE_NAME),
             schema=sql.Literal(SCHEMA))
    
    return output

def column_names(column_name_sql_output):

    column_name_string = ''

    for column in column_name_sql_output:
        if column_name_sql_output.index(column) == len(column_name_sql_output)-1:
            column_name_string = column_name_string + str(column[0]) + '=our_staging_table.' + str(column[0]) + ' '
        else:
            column_name_string = column_name_string + str(column[0]) + '=our_staging_table.' + str(column[0]) +', '
            
    return column_name_string

def update_table(TABLE_NAME, COLUMN_NAME_STRING):
    
    output = sql.SQL(
        """
        UPDATE {table}
        SET {column_name}
        FROM our_staging_table
        WHERE {table}.id = our_staging_table.id
        """
    ).format(table=sql.Identifier(TABLE_NAME),
            column_name=sql.SQL(COLUMN_NAME_STRING))
    
    return output

def delete_updated_rows_from_temp(TABLE_NAME):

    output = sql.SQL(
        """
        DELETE FROM our_staging_table USING {table} WHERE our_staging_table.id = {table}.id
        """
    ).format(table=sql.Identifier(TABLE_NAME))
    
    return output

def insert_into_table(TABLE_NAME):

    output = sql.SQL(
        """
        INSERT INTO {table} SELECT * FROM our_staging_table;
        """
    ).format(table=sql.Identifier(TABLE_NAME))
    
    return output


def drop_temp_table():

    output = "DROP TABLE our_staging_table;"

    return output

def connect_to_postgres():

    """Connect to PostgreSQL instance"""
    try:
        pg_conn = psycopg2.connect(
            database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT, options="-c search_path="+SCHEMA
        )
        return pg_conn
    except Exception as e:
        print(f"Unable to connect to PostgreSQL. Error {e}")
        sys.exit(1)

def load_data_into_postgres(pg_conn,create_table_sql,table):

    """Load data from CSV into Postgres"""
    with pg_conn:

        cur = pg_conn.cursor()
        cur.execute(create_table_sql)
        cur.execute(create_temp_table(table))
        cur.copy_expert(sql_copy_to_temp(table)[0], sql_copy_to_temp(table)[1])
        cur.execute(column_names_sql(table))
        columns = cur.fetchall()
        column_name = column_names(columns)
        cur.execute(update_table(table,column_name))
        cur.execute(delete_updated_rows_from_temp(table))
        cur.execute(insert_into_table(table))
        cur.execute(drop_temp_table())

        # Commit only at the end, so we won't end up
        # with a temp table and deleted main table if something fails
        pg_conn.commit()

def main():
    """Upload file fromm CSV to PostgreSQL"""
    validate_input(output_name)
    pg_conn = connect_to_postgres()

    load_data_into_postgres(pg_conn, sql_create_teams_table, TEAMS_TABLE)
    load_data_into_postgres(pg_conn, sql_create_players_table, PLAYERS_TABLE)
    load_data_into_postgres(pg_conn, sql_create_gameweek_table, GAMEWEEK_TABLE)
    load_data_into_postgres(pg_conn, sql_create_fixtures_table, FIXTURES_TABLE)
    load_data_into_postgres(pg_conn, sql_create_myteam_table, MY_TEAM_TABLE)
    load_data_into_postgres(pg_conn, sql_create_myteamtransfers_table, MY_TEAM_TANSFERS_TABLE)

if __name__ == "__main__":
    main()