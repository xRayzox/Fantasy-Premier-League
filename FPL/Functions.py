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
def get_player_history(player_id):
    url = PLAYER_HISTORY_URL.format(player_id=player_id)
    response = requests.get(url)
    if response.status_code == 200:
        player_data = response.json()
        return player_data.get('history', [])  # Return only the 'history' list
    else:
        response.raise_for_status()

# Fetch all data as JSON
def fetch_fpl_data():
    # Fetch bootstrap-static data (players, teams, gameweeks)
    data = get_fpl_data()

    # Extract Players, Teams, and Gameweeks
    players_data = data['elements']
    teams_data = data['teams']
    gameweeks_data = data['events']

    # Fetch fixtures data
    fixtures_data = get_fixtures_data()

    # Fetch Player History for all players
    player_history_data = {}
    for player in players_data:  # Loop through all players
        player_id = player['id']
        player_history = get_player_history(player_id)
        player_history_data[player_id] = player_history  # Store only the history list

    # Store all fetched data in a dictionary
    fpl_data = {
        'players': players_data,
        'teams': teams_data,
        'gameweeks': gameweeks_data,
        'fixtures': fixtures_data,
        'player_history': player_history_data,
    }

    # Return the full FPL data as JSON
    return fpl_data



