import requests
import json
import os 

from bs4 import BeautifulSoup

BASE_URL = "https://fantasy.premierleague.com/api/"

def get_fpl_api_data(url):
    """Fetches data from the FPL API."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_data(data, filename, data_type="api"):
    """Saves data to a file (JSON)."""
    filepath = os.path.join('data/raw', filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def get_bootstrap_static():
    
    url = BASE_URL + "bootstrap-static/"
    data = get_fpl_api_data(url)
    save_data(data, 'bootstrap_static.json')
    return data

def get_player_summary(player_id):
    """Retrieves player summary data for a given player ID."""
    url = f"{BASE_URL}element-summary/{player_id}/"
    data = get_fpl_api_data(url)
    save_data(data, f'player_summary/{player_id}.json')
    return data

def get_event_live(event_id):
    """Retrieves live data for a specific gameweek (event)."""
    url = f"{BASE_URL}event/{event_id}/live/"
    data = get_fpl_api_data(url)
    save_data(data, f'events/event_{event_id}_live.json')
    return data

def get_teams():
    bootstrap_data = get_bootstrap_static() 
    teams_data = bootstrap_data.get('teams', []) # Extract teams list
    save_data(teams_data, 'teams.json')
    return teams_data

def get_fixtures():
    """Retrieves fixture data from bootstrap-static."""
    bootstrap_data = get_bootstrap_static()
    fixtures_data = bootstrap_data.get('events', []) # Extract fixtures list
    save_data(fixtures_data, 'fixtures.json')
    return fixtures_data

def get_players():
    """Retrieves player data from bootstrap-static."""
    bootstrap_data = get_bootstrap_static()
    players_data = bootstrap_data.get('elements', [])
    save_data(players_data, 'players.json')
    return players_data

def get_events():
    """Retrieves event (gameweek) data from bootstrap-static."""
    bootstrap_data = get_bootstrap_static()
    events_data = bootstrap_data.get('events', [])
    save_data(events_data, 'events.json')
    return events_data