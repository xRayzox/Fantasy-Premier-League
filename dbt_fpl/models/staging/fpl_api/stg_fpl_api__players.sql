with

source as (

    select * from {{ source('fpl_api','players') }}

),

players as (

    select

        id as player_id,
        chance_of_playing_next_round::integer as chance_of_playing_next_gameweek,
        chance_of_playing_this_round::integer as chance_of_playing_this_gameweek,
        dreamteam_count,
        ep_next as expected_points_for_next_gameweek,
        ep_this as expected_points_for_this_gameweek,
        event_points,
        first_name,
        form,
        in_dreamteam,
        news,
        news_added as news_added_time,
        now_cost/10 as now_cost,
        points_per_game,
        second_name as last_name,
        selected_by_percent,
        team as team_id,
        total_points,
        transfers_in,
        transfers_in_event as transfers_in_this_gameweek,
        transfers_out,
        transfers_out_event as transfers_out_this_gameweek,
        value_form,
        value_season,
        web_name,
        minutes,
        goals_scored,
        assists,
        clean_sheets,
        goals_conceded,
        own_goals,
        penalties_saved,
        penalties_missed,
        yellow_cards,
        red_cards,
        saves,
        bonus,
        starts,
        expected_goals,
        expected_assists,
        expected_goal_involvement,
        expected_goals_conceded,
        expected_goals_per_90,
        saves_per_90,
        expected_assists_per_90,
        expected_goal_involvements_per_90,
        expected_goals_conceded_per_90,
        goals_conceded_per_90,
        starts_per_90,
        clean_sheets_per_90,
        position

    from source
),

final as (

    select * from players

)

select * from final