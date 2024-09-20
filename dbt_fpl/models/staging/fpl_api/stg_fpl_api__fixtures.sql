with

source as (

    select * from {{ source('fpl_api','fixtures') }}

),

fixtures as (

    select

        id as fixture_id,
		gameweek as gameweek_id,
		finished,
		kickoff_time,
		minutes,
		started,
		away_team as away_team_id,
		away_team_score,
		home_team as home_team_id,
		home_team_score,
		home_team_difficulty,
		away_team_difficulty

    from source
),

final as (

    select * from fixtures

)

select * from final