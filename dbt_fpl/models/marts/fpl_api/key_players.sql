with
teams as (

    select * from {{ ref('stg_fpl_api__teams') }}

),

int_players_top_ranked as (

    select * from {{ ref('int_players_top_ranked') }}

),

gameweek_info as (

    select * from {{ ref('gameweek_info')}}

),

key_players as (

    select

    chance_of_playing_next_gameweek as availability,
    expected_points_for_next_gameweek as xPoints,
    now_cost as cost,
    selected_by_percent,
    total_points,
    web_name,
    position,
    teams.short_name as team

    from int_players_top_ranked 

    left join teams

    on int_players_top_ranked.team_id = teams.team_id

),

final as (

    select * from key_players

)

select * from final