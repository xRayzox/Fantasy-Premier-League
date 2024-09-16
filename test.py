from FPL import Functions as fn
import json
# Function to extract and save team data
def save_teams_data(filename='teams_data.json'):
    data = fn.get_fpl_data()  # Get the full data
    teams = data.get(teams", [])  # Extract the teams data
    print(teams)
    with open(filename, 'w') as f:
        json.dump(teams, f, indent=4)  # Save to file with pretty formatting

if __name__ == "__main__":
    save_teams_data()  # Save the teams data to 'teams_data.json'