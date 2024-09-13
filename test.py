from FPL import Functions as fn

data = fn.get_fpl_data()
players_data = data['elements']
player_ids = [player['id'] for player in players_data]
player=fn.get_players_history(player_ids)

print(player)

