import requests
import pandas as pd

class FPLElementType:
    def __init__(self, data):
        self.id = data['id']
        self.plural_name = data['plural_name']
        self.plural_name_short = data['plural_name_short']
        self.singular_name = data['singular_name']
        self.singular_name_short = data['singular_name_short']
        self.squad_select = data['squad_select']
        self.squad_min_select = data['squad_min_select']
        self.squad_max_select = data['squad_max_select']
        self.squad_min_play = data['squad_min_play']
        self.squad_max_play = data['squad_max_play']
        self.ui_shirt_specific = data['ui_shirt_specific']
        self.sub_positions_locked = data['sub_positions_locked']
        self.element_count = data['element_count']

    def to_dataframe(self):
        return pd.DataFrame([self.__dict__])

class FPLElement:
    def __init__(self, data):
        self.chance_of_playing_next_round = data.get('chance_of_playing_next_round')
        self.chance_of_playing_this_round = data.get('chance_of_playing_this_round')
        self.code = data['code']
        self.cost_change_event = data['cost_change_event']
        self.cost_change_event_fall = data['cost_change_event_fall']
        self.cost_change_start = data['cost_change_start']
        self.cost_change_start_fall = data['cost_change_start_fall']
        self.dreamteam_count = data['dreamteam_count']
        self.element_type = data['element_type']
        self.ep_next = data['ep_next']
        self.ep_this = data['ep_this']
        self.event_points = data['event_points']
        self.first_name = data['first_name']
        self.form = data['form']
        self.id = data['id']
        self.in_dreamteam = data['in_dreamteam']
        self.news = data['news']
        self.news_added = data['news_added']
        self.now_cost = data['now_cost']
        self.photo = data['photo']
        self.points_per_game = data['points_per_game']
        self.second_name = data['second_name']
        self.selected_by_percent = data['selected_by_percent']
        self.special = data['special']
        self.squad_number = data['squad_number']
        self.status = data['status']
        self.team = data['team']
        self.team_code = data['team_code']
        self.total_points = data['total_points']
        self.transfers_in = data['transfers_in']
        self.transfers_in_event = data['transfers_in_event']
        self.transfers_out = data['transfers_out']
        self.transfers_out_event = data['transfers_out_event']
        self.value_form = data['value_form']
        self.value_season = data['value_season']
        self.web_name = data['web_name']
        self.minutes = data['minutes']
        self.goals_scored = data['goals_scored']
        self.assists = data['assists']
        self.clean_sheets = data['clean_sheets']
        self.goals_conceded = data['goals_conceded']
        self.own_goals = data['own_goals']
        self.penalties_saved = data['penalties_saved']
        self.penalties_missed = data['penalties_missed']
        self.yellow_cards = data['yellow_cards']
        self.red_cards = data['red_cards']
        self.saves = data['saves']
        self.bonus = data['bonus']
        self.bps = data['bps']
        self.influence = data['influence']
        self.creativity = data['creativity']
        self.threat = data['threat']
        self.ict_index = data['ict_index']
        self.starts = data['starts']
        self.expected_goals = data['expected_goals']
        self.expected_assists = data['expected_assists']
        self.expected_goal_involvements = data['expected_goal_involvements']
        self.expected_goals_conceded = data['expected_goals_conceded']
        self.influence_rank = data['influence_rank']
        self.influence_rank_type = data['influence_rank_type']
        self.creativity_rank = data['creativity_rank']
        self.creativity_rank_type = data['creativity_rank_type']
        self.threat_rank = data['threat_rank']
        self.threat_rank_type = data['threat_rank_type']
        self.ict_index_rank = data['ict_index_rank']
        self.ict_index_rank_type = data['ict_index_rank_type']
        self.corners_and_indirect_freekicks_order = data['corners_and_indirect_freekicks_order']
        self.corners_and_indirect_freekicks_text = data['corners_and_indirect_freekicks_text']
        self.direct_freekicks_order = data['direct_freekicks_order']
        self.direct_freekicks_text = data['direct_freekicks_text']
        self.penalties_order = data['penalties_order']
        self.penalties_text = data['penalties_text']
        self.expected_goals_per_90 = data['expected_goals_per_90']
        self.saves_per_90 = data['saves_per_90']
        self.expected_assists_per_90 = data['expected_assists_per_90']
        self.expected_goal_involvements_per_90 = data['expected_goal_involvements_per_90']
        self.expected_goals_conceded_per_90 = data['expected_goals_conceded_per_90']
        self.goals_conceded_per_90 = data['goals_conceded_per_90']
        self.now_cost_rank = data['now_cost_rank']
        self.now_cost_rank_type = data['now_cost_rank_type']
        self.form_rank = data['form_rank']
        self.form_rank_type = data['form_rank_type']
        self.points_per_game_rank = data['points_per_game_rank']
        self.points_per_game_rank_type = data['points_per_game_rank_type']
        self.selected_rank = data['selected_rank']
        self.selected_rank_type = data['selected_rank_type']
        self.starts_per_90 = data['starts_per_90']
        self.clean_sheets_per_90 = data['clean_sheets_per_90']

        # Additional fields from your example
        self.region = data['region']

    def to_dataframe(self):
        return pd.DataFrame([self.__dict__])

class FPLTeam:
    def __init__(self, data):
        self.code = data['code']
        self.draw = data['draw']
        self.form = data['form']
        self.id = data['id']
        self.loss = data['loss']
        self.name = data['name']
        self.played = data['played']
        self.points = data['points']
        self.position = data['position']
        self.short_name = data['short_name']
        self.strength = data['strength']
        self.team_division = data['team_division']
        self.unavailable = data['unavailable']
        self.win = data['win']
        self.strength_overall_home = data['strength_overall_home']
        self.strength_overall_away = data['strength_overall_away']
        self.strength_attack_home = data['strength_attack_home']
        self.strength_attack_away = data['strength_attack_away']
        self.strength_defence_home = data['strength_defence_home']
        self.strength_defence_away = data['strength_defence_away']
        self.pulse_id = data['pulse_id'] 
    
    def to_dataframe(self):
        return pd.DataFrame([self.__dict__])

class FPLEvent:
    def __init__(self, data):
        self.id = data['id']
        self.name = data['name']
        self.deadline_time = data['deadline_time']
        self.average_entry_score = data['average_entry_score']
        self.finished = data['finished']
        self.data_checked = data['data_checked']
        self.highest_scoring_entry = data['highest_scoring_entry']
        self.deadline_time_epoch = data['deadline_time_epoch']
        self.deadline_time_game_offset = data['deadline_time_game_offset']
        self.highest_score = data['highest_score']
        self.is_previous = data['is_previous']
        self.is_current = data['is_current']
        self.is_next = data['is_next']
        self.cup_leagues_created = data['cup_leagues_created']
        self.h2h_ko_matches_created = data['h2h_ko_matches_created']
        self.chip_plays = data['chip_plays']
        self.most_selected = data['most_selected']
        self.most_transferred_in = data['most_transferred_in']
        self.top_element = data['top_element']
        self.top_element_info = data['top_element_info']
        self.transfers_made = data['transfers_made']
        self.most_captained = data['most_captained']
        self.most_vice_captained = data['most_vice_captained']
        self.ranked_count = data['ranked_count']

        # Additional fields from your example
        self.release_time = data['release_time']

    def to_dataframe(self):
        return pd.DataFrame([self.__dict__])

class FPLFixture:
    def __init__(self, data):
        self.code = data['code']
        self.event = data['event']
        self.finished = data['finished']
        self.finished_provisional = data['finished_provisional']
        self.id = data['id']
        self.kickoff_time = data['kickoff_time']
        self.minutes = data['minutes']
        self.provisional_start_time = data['provisional_start_time']
        self.started = data['started']
        self.team_a = data['team_a']
        self.team_a_score = data['team_a_score']
        self.team_h = data['team_h']
        self.team_h_score = data['team_h_score']
        self.team_h_difficulty = data['team_h_difficulty']
        self.team_a_difficulty = data['team_a_difficulty']
        self.pulse_id = data['pulse_id']
        # Handle the nested 'stats' data
        self.stats_df = pd.DataFrame(data['stats'])  # Create a DataFrame directly from 'stats'
        # If you want to flatten the 'a' and 'h' nested data for each stat:
        self.home_stats_df = pd.json_normalize(data['stats'], record_path=['h'], meta=['identifier'])
        self.away_stats_df = pd.json_normalize(data['stats'], record_path=['a'], meta=['identifier'])

    def to_dataframe(self):
        # Exclude the nested stats DataFrames
        exclude_keys = ['stats_df', 'home_stats_df', 'away_stats_df']
        return pd.DataFrame([ {k: v for k, v in self.__dict__.items() if k not in exclude_keys} ])

class FPLHistory:
    def __init__(self, data):
        self.element = data['element']
        self.fixture = data['fixture']
        self.opponent_team = data['opponent_team']
        self.total_points = data['total_points']
        self.was_home = data['was_home']
        self.kickoff_time = data['kickoff_time']
        self.team_h_score = data['team_h_score']
        self.team_a_score = data['team_a_score']
        self.round = data['round']
        self.minutes = data['minutes']
        self.goals_scored = data['goals_scored']
        self.assists = data['assists']
        self.clean_sheets = data['clean_sheets']
        self.goals_conceded = data['goals_conceded']
        self.own_goals = data['own_goals']
        self.penalties_saved = data['penalties_saved']
        self.penalties_missed = data['penalties_missed']
        self.yellow_cards = data['yellow_cards']
        self.red_cards = data['red_cards']
        self.saves = data['saves']
        self.bonus = data['bonus']
        self.bps = data['bps']
        self.influence = data['influence']
        self.creativity = data['creativity']
        self.threat = data['threat']
        self.ict_index = data['ict_index']
        self.value = data['value']
        self.transfers_balance = data['transfers_balance']
        self.selected = data['selected']
        self.transfers_in = data['transfers_in']
        self.transfers_out = data['transfers_out']
        self.starts = data['starts']
        self.expected_goals = data['expected_goals']
        self.expected_assists = data['expected_assists']
        self.expected_goal_involvements = data['expected_goal_involvements']
        self.expected_goals_conceded = data['expected_goals_conceded']

    def to_dataframe(self):
        return pd.DataFrame([self.__dict__])

def get_bootstrap_static_data():
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    data = response.json()
    return data

def get_fixtures_data():
    url = "https://fantasy.premierleague.com/api/fixtures/"
    response = requests.get(url)
    data = response.json()
    return data

def get_player_history_data(player_id):
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    response = requests.get(url)
    try:
        data = response.json()
        return data['history'] # Return only the history data
    except:
        print(f"Error fetching data for player ID: {player_id}")
        return [] # Return empty list if there's an error

if __name__ == "__main__":

    # 1. Scrape Bootstrap Static Data 
    bootstrap_data = get_bootstrap_static_data()

    # 2. Create DataFrames for Static Tables
    element_types_df = pd.concat([FPLElementType(item).to_dataframe() for item in bootstrap_data['element_types']], ignore_index=True)
    elements_df = pd.concat([FPLElement(item).to_dataframe() for item in bootstrap_data['elements']], ignore_index=True)
    teams_df = pd.concat([FPLTeam(item).to_dataframe() for item in bootstrap_data['teams']], ignore_index=True)
    events_df = pd.concat([FPLEvent(item).to_dataframe() for item in bootstrap_data['events']], ignore_index=True)

    # 3. Scrape Fixtures Data 
    fixtures_data = get_fixtures_data() 

    # 4. Create DataFrame for Fixtures 
    fixtures_df = pd.concat([FPLFixture(item).to_dataframe() for item in fixtures_data], ignore_index=True) 

    # 5. Scrape and Create DataFrames for Player History
    player_ids = elements_df['id'].tolist()  # Get player IDs from the elements table
    all_player_history = []
    for player_id in player_ids:
        player_history = get_player_history_data(player_id)
        all_player_history.extend([FPLHistory(item).to_dataframe() for item in player_history])

    history_df = pd.concat(all_player_history, ignore_index=True)

    # 6. (Optional) Save DataFrames to CSV files 
    element_types_df.to_csv("element_types.csv", index=False)
    elements_df.to_csv("elements.csv", index=False)
    teams_df.to_csv("teams.csv", index=False)
    events_df.to_csv("events.csv", index=False)
    fixtures_df.to_csv("fixtures.csv", index=False)
    history_df.to_csv("history.csv", index=False) 