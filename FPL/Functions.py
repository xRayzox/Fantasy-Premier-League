import requests
import json

# Base URLs for the FPL API
BASE_URL = "https://fantasy.premierleague.com/api"
BOOTSTRAP_STATIC_URL = f"{BASE_URL}/bootstrap-static/"
FIXTURES_URL = f"{BASE_URL}/fixtures/"
PLAYER_HISTORY_URL = f"{BASE_URL}/element-summary/{{player_id}}/"

# Function to get FPL Bootstrap Static Data (players, teams, gameweeks)
def get_fpl_data():
    response = requests.get(BOOTSTRAP_STATIC_URL)
    if response.status_code == 200:
        return response.json()  # Return JSON data
    else:
        response.raise_for_status()

# Function to get FPL Fixtures Data
def get_fixtures_data():
    response = requests.get(FIXTURES_URL)
    if response.status_code == 200:
        return response.json()  # Return JSON data
    else:
        response.raise_for_status()

# Function to get player history (only the 'history' list for a specific player)
import requests

def get_players_history(player_ids):
    players_history = []
    for player_id in player_ids:
        url = PLAYER_HISTORY_URL.format(player_id=player_id)
        response = requests.get(url)
        if response.status_code == 200:
            player_data = response.json()
            histories= player_data.get('history', [])
            players_history.extend(histories)  # Store history with player_id as key
    return players_history






