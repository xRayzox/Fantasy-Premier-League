import requests
import pandas as pd
import numpy as np
import datetime
import sys
import os
import configparser
import pathlib
from validation import validate_input

"""
First Part of Airflow DAG. Takes in one command line argument of format YYYYMMDD.
Script will connect to FPL APIs and extract FPL data like gameweek info, fixtures, teams, players and manager info.
"""

#Read Configuration File
parser = configparser.ConfigParser()
config_file = "/opt/airflow/dbt/configuration.conf"
parser.read(config_file)

#Configuration Variables
MY_TEAM_ID = parser.get("FPL_Configuration","my_team_id")

#FPL Endpoints
base_url = 'https://fantasy.premierleague.com/api/'
general_info = 'bootstrap-static/'
fixtures = 'fixtures/'
myteam = 'entry/'+MY_TEAM_ID+'/'

# Use command line argument as output file name and also store as column value
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Error with file input. Error {e}")
    sys.exit(1)

def general_info_api_connect():
    """Connect to General Info (bootstrap-static) API"""

    try:
        r = requests.get(base_url+general_info).json()
        return r
    except Exception as e:
        print(f"Error:{e}")
        sys.exit(1)

def general_info_extract(request):
    """Extract Gameweek, Teams and Players data"""
    
    #Gameweek Dataframe
    gameweek = pd.DataFrame(request['events']).set_index(['id'])
    
    #Teams Dataframe
    teams = pd.DataFrame(request['teams']).set_index(['id'])
    
    #Select Columns for Teams Dataframe
    columns = ['name','short_name','strength','strength_overall_home','strength_overall_away','strength_attack_home','strength_attack_away','strength_defence_home',
               'strength_defence_away']
    teams = teams[columns]
    
    #Element Types Dataframe
    element_types = pd.DataFrame(request['element_types']).set_index(['id'])
    
    #Players Dataframe
    players = pd.DataFrame(request['elements']).set_index(['id'])
    
    #Join Players and Element Type Dataframe
    players = players.join(element_types,on='element_type',how='left')
    
    #Select Columns for Players Dataframe
    columns = ['chance_of_playing_next_round','chance_of_playing_this_round','dreamteam_count','ep_next','ep_this','event_points','first_name','form','in_dreamteam','news',
               'news_added','now_cost','points_per_game','second_name','selected_by_percent','team','total_points','transfers_in','transfers_in_event','transfers_out',
               'transfers_out_event','value_form','value_season','web_name','minutes','goals_scored','assists','clean_sheets','goals_conceded','own_goals','penalties_saved',
               'penalties_missed','yellow_cards','red_cards','saves','bonus','starts','expected_goals','expected_assists','expected_goal_involvements','expected_goals_conceded',
               'expected_goals_per_90','saves_per_90','expected_assists_per_90','expected_goal_involvements_per_90','expected_goals_conceded_per_90','goals_conceded_per_90',
               'starts_per_90','clean_sheets_per_90','singular_name_short']
    players = players[columns]
    
    return gameweek, teams, players

def gameweek_transform(request_dataframe):
    """Unpacks columns, converts float to integer values and selects relevant columns"""

    #Define columns to added as lists
    benchboost = [float('nan')]*38
    triple_captain = [float('nan')]*38
    wildcard = [float('nan')]*38
    freehit = [float('nan')]*38
    top_element_points = [float('nan')]*38
    
    #Add row values to Benchboost, Triple Captain and Freehit lists by unpacking 'chip_plays'
    for x,l in enumerate(request_dataframe['chip_plays']):
        if l != []:
            for i in range(len(l)):
                if l[i]['chip_name']=='bboost':
                    benchboost[x] = l[i]['num_played']
                elif l[i]['chip_name']=='3xc':
                    triple_captain[x] = l[i]['num_played']
                elif l[i]['chip_name']=='freehit':
                    freehit[x] = l[i]['num_played']
                else:
                    wildcard[x] = l[i]['num_played']
    
    #Add row values to top_element_points list by unpacking 'top_element_info'
    for x,l in enumerate(request_dataframe['top_element_info']):
        if l != None:
            top_element_points[x] = l['points']
    
    #Convert lists to Dataframes
    benchboost = pd.DataFrame(benchboost,columns=['Benchboost'],index=request_dataframe.index)
    triple_captain = pd.DataFrame(triple_captain,columns=['Triple Captain'],index=request_dataframe.index)
    wildcard = pd.DataFrame(wildcard,columns=['Wildcard'],index=request_dataframe.index)
    freehit = pd.DataFrame(freehit,columns=['Freehit'],index=request_dataframe.index)
    top_element_points = pd.DataFrame(top_element_points,columns=['top_element_points'],index=request_dataframe.index)

    #Concat all Dataframes
    request_dataframe_transformed = pd.concat([request_dataframe, benchboost, triple_captain, wildcard, freehit, top_element_points], axis=1)
    
    #Select Columns
    columns = ['name','deadline_time','average_entry_score','finished','data_checked','highest_scoring_entry','highest_score','is_previous','is_current',
               'is_next','most_selected','most_transferred_in','top_element','transfers_made','most_captained','most_vice_captained','Benchboost','Triple Captain',
               'Wildcard','Freehit','top_element_points']
    request_dataframe_transformed = request_dataframe_transformed[columns]

    #Convert float to integers where it makes sense
    for col in ['highest_scoring_entry','highest_score','most_selected','most_transferred_in','top_element','most_captained','most_vice_captained','Benchboost','Triple Captain',
                'Wildcard','Freehit','top_element_points']:
        request_dataframe_transformed[col] = request_dataframe_transformed[col].astype('Int64')
    
    return request_dataframe_transformed

def fixtures_api_connect():
    """Connect to Fixtures API"""

    try:
        r = requests.get(base_url+fixtures).json()
        return r
    except Exception as e:
        print(f"Error:{e}")
        sys.exit(1)

def fixtures_extract(request):
    """Extract Fixutes Data"""

    #Fixtures Dataframe
    fixture = pd.DataFrame(request).set_index(['id']).sort_index()

    #Select Columns for Fixtures Dataframe
    columns = ['event','finished','kickoff_time','minutes','started','team_a','team_a_score','team_h','team_h_score','team_h_difficulty','team_a_difficulty']
    fixture = fixture[columns]

    #Convert float to integer
    for col in ['event','team_a_score','team_h_score']:
        fixture[col] = fixture[col].astype('Int64')
    
    return fixture

def myteam_api_connect():
    """Connect to My Team (entry/MY_TEAM_ID/) API"""

    try:
        r = requests.get(base_url+myteam+'history/').json()
        return r
    except Exception as e:
        print(f"Error:{e}")
        sys.exit(1)

def myteam_extract(request):
    """Extract My Team Data"""

    #My Team Dataframe
    my_team = pd.DataFrame(request['current'])
    my_team.set_index(my_team['event'],inplace=True)
    
    #Select Columns for My Team Dataframe
    columns = ['event','points','total_points','rank','rank_sort','overall_rank','bank','value','event_transfers','event_transfers_cost','points_on_bench']
    my_team = my_team[columns]
    
    # Rename index and event column
    my_team.rename(columns={'event':'gameweek'},inplace=True)
    my_team.index.name = 'id'
    
    return my_team

def myteam_transfers_api_connect():
    """Connect to My Team Transfers (entry/MY_TEAM_ID/) API"""

    try:
        r = requests.get(base_url+myteam+'transfers/').json()
        return r
    except Exception as e:
        print(f"Error:{e}")
        sys.exit(1)

def myteam_transfers_extract(request):
    """Extract My Team Transfers Data"""
    
    #My Team Transfers Dataframe
    my_team_transfers = pd.DataFrame(request).sort_values(['time'])
    
    #Select Columns for My Team Transfers Dataframe
    columns = ['element_in','element_in_cost','element_out','element_out_cost','event','time']
    my_team_transfers = my_team_transfers[columns]
    
    #Set Index
    my_team_transfers.index = np.arange(1, len(my_team_transfers) + 1)
    my_team_transfers.index.name = 'id'
    
    return my_team_transfers

def load_to_csv(extracted_data_df,name):
    """Save extracted data to CSV file in /tmp folder"""
    tmp_dir = f"/opt/airflow/fpl_functions/tmp"
    extracted_data_df.to_csv(f"{tmp_dir}/{name}_{output_name}.csv")

def main():

    validate_input(output_name)
    
    general_info_instance = general_info_api_connect()
    general_info_extracted_data = general_info_extract(general_info_instance)
    gameweek_transformed_data = gameweek_transform(general_info_extracted_data[0])
    load_to_csv(gameweek_transformed_data,"gameweek")
    load_to_csv(general_info_extracted_data[1],"teams")
    load_to_csv(general_info_extracted_data[2],"players")

    fixtures_instance = fixtures_api_connect()
    fixtures_extracted_data = fixtures_extract(fixtures_instance)
    load_to_csv(fixtures_extracted_data,"fixtures")
    
    myteam_instance = myteam_api_connect()
    myteam_extracted_data = myteam_extract(myteam_instance)
    load_to_csv(myteam_extracted_data,"my_team")
    
    myteam_transfers_instance = myteam_transfers_api_connect()
    myteam_transfers_extracted_data = myteam_transfers_extract(myteam_transfers_instance)
    load_to_csv(myteam_transfers_extracted_data,"my_team_transfers")

if __name__ == "__main__":
    main()