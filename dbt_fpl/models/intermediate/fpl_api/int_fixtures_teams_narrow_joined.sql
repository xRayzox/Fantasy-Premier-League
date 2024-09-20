with

fixtures as (

    select * from {{ ref('stg_fpl_api__fixtures') }}

),

teams as (

    select * from {{ ref('stg_fpl_api__teams') }}

),

home_fixtures as (

    select 
        fixture_id,
        gameweek_id,
        finished,
        kickoff_time,
        minutes,started,
        home_team_id as team,
        home_team_score as score,
        home_team_difficulty as difficulty,
        'home' as type

    from fixtures order by fixture_id

),

away_fixtures as (

    select
        fixture_id,
        gameweek_id,
        finished,
        kickoff_time,
        minutes,
        started,
        away_team_id as team,
        away_team_score as score,
        away_team_difficulty as difficulty,
        'away' as type

    from fixtures order by fixture_id
	
),

home_away_fixtures_joined as (

    select 
        home_fixtures.*,
        away_fixtures.team as opponent

    from home_fixtures
    left join away_fixtures on home_fixtures.fixture_id = away_fixtures.fixture_id
),

away_home_fixtures_joined as (

    select 
        away_fixtures.*,
        home_fixtures.team as opponent
        
    from away_fixtures
    left join home_fixtures on away_fixtures.fixture_id = home_fixtures.fixture_id
),

fixtures_narrow as (

    select * from home_away_fixtures_joined 
    union
    select * from away_home_fixtures_joined

),

fixtures_teams_narrow_joined as (


    select 
        fixtures_narrow.*,
        team.short_name as team_name,
        opponent.short_name as opponent_name

    from fixtures_narrow
    
    left join teams as team
    on fixtures_narrow.team = team.team_id

    left join teams as opponent
    on fixtures_narrow.opponent = opponent.team_id

),

final as (

    select
        fixtures_teams_narrow_joined.fixture_id,
        fixtures_teams_narrow_joined.gameweek_id,
        fixtures_teams_narrow_joined.finished,
        fixtures_teams_narrow_joined.kickoff_time,
        fixtures_teams_narrow_joined.minutes,
        fixtures_teams_narrow_joined.started,
        fixtures_teams_narrow_joined.team_name as team,
        fixtures_teams_narrow_joined.score,
        fixtures_teams_narrow_joined.difficulty,
        fixtures_teams_narrow_joined.type,
        fixtures_teams_narrow_joined.opponent_name as opponent
    
    from fixtures_teams_narrow_joined
    where gameweek_id is not null
    order by fixture_id

)

select * from final