with

fixtures as (

    select * from {{ ref('stg_fpl_api__fixtures') }}

),

teams as (

    select * from {{ ref('stg_fpl_api__teams') }}

),

fixtures_teams_joined as (

    select 
        fixtures.*,
        home_team.short_name as home_team,
        away_team.short_name as away_team

    from fixtures
    
    left join teams as home_team
    on fixtures.home_team_id = home_team.team_id

    left join teams as away_team
    on fixtures.away_team_id = away_team.team_id

),

final as (

    select
        fixture_id,
        gameweek_id,
        finished,
        kickoff_time,
        minutes,
        started,
        away_team,
        away_team_score,
        away_team_difficulty,
        home_team,
        home_team_score,
        home_team_difficulty

    from fixtures_teams_joined

    order by fixture_id

)

select * from final