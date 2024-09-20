with

gameweek as (

    select * from {{ ref('stg_fpl_api__gameweek') }}

),

players as (

    select * from {{ ref('stg_fpl_api__players') }}

),

gameweek_players_joined as (

    select

        gameweek.*,
        player_msl.web_name as most_selected_name,
        player_mti.web_name as most_transferred_in_name,
        player_msc.web_name as most_scoring_player_name,
        player_mc.web_name as most_captained_name,
        player_mvc.web_name as most_vice_capitained_name

    from gameweek

    left join players as player_msl on gameweek.most_selected_player_id = player_msl.player_id
    left join players as player_mti on gameweek.most_transferred_in_player_id = player_mti.player_id
    left join players as player_msc on gameweek.most_scoring_player_id = player_msc.player_id
    left join players as player_mc on gameweek.most_captained_player_id = player_mc.player_id
    left join players as player_mvc on gameweek.most_vice_captained_player_id = player_mvc.player_id

),

final as (

    select
		gameweek_id,
		gameweek_name,
		deadline_time,
		average_score,
		finished,
		data_checked,
		highest_scoring_manager,
		highest_score,
		is_previous,
		is_current,
		is_next,
		most_selected_name as most_selected,
		most_transferred_in_name as most_transferred_in,
		most_scoring_player_name as most_scoring,
		transfers_made,
		most_captained_name as most_captained,
		most_vice_capitained_name as most_vice_captained,
		benchboost,
		triple_captain,
		wildcard,
		freehit,
		most_scoring_player_points
  
    from gameweek_players_joined order by gameweek_id

)

select * from final