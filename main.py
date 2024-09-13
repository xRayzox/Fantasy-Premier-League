from FPL import Functions as fn
def main():
    bootstrap_data = fn.get_bootstrap_static()  # Get bootstrap data once
    fn.get_teams()
    fn.get_players()
    fn.get_fixtures()
    fn.get_events()
   
    # Get player IDs from bootstrap data
    player_ids = [player['id'] for player in bootstrap_data['elements']] 
    events_ids = [player['id'] for event in fn.get_events()] 
    # Get player summaries for all players
    for player_id in player_ids:
        fn.get_player_summary(player_id)

    # Example: Get data for the first 3 events 
    for event_id in events_ids:
        fn.get_event_live(event_id) 

if __name__ == "__main__":
    main()