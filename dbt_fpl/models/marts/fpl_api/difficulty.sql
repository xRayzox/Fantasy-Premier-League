{% set sql_statement %}
    select gameweek_id from {{ ref('int_gameweek_players_joined') }} where is_current = true
{% endset %}

{% set current_gameweek = dbt_utils.get_single_value(sql_statement) %}

with 

int_fixtures_teams_narrow_joined as (

    select * from {{ ref('int_fixtures_teams_narrow_joined') }}

),

difficulty as (

    select 
        gameweek_id,
        team,
        SUM(difficulty) as total_difficulty,
        COUNT(fixture_id) as number_of_fixtures
    
    from int_fixtures_teams_narrow_joined

    where gameweek_id >= {{ current_gameweek|int + 1 }}
    
    group by gameweek_id,team

),

difficulty_joined as (

    select
        difficulty.*,
        iftnj.opponent,
        iftnj.type
    
    from difficulty

    left join int_fixtures_teams_narrow_joined as iftnj

    on difficulty.team = iftnj.team and difficulty.gameweek_id = iftnj.gameweek_id

),

final as (

    select * from difficulty_joined

    order by gameweek_id,total_difficulty
)

select * from final