with 

int_fixtures_teams_narrow_joined as (

    select * from {{ ref('int_fixtures_teams_narrow_joined') }}

),

fixture_id_pivot as (

        select
            team,
            {{ gameweek_columns(1) }},
            {{ gameweek_columns(2) }},
            {{ gameweek_columns(3) }},
            {{ gameweek_columns(4) }},
            {{ gameweek_columns(5) }},
            {{ gameweek_columns(6) }},
            {{ gameweek_columns(7) }},
            {{ gameweek_columns(8) }},
            {{ gameweek_columns(9) }},
            {{ gameweek_columns(10) }},
            {{ gameweek_columns(11) }},
            {{ gameweek_columns(12) }},
            {{ gameweek_columns(13) }},
            {{ gameweek_columns(14) }},
            {{ gameweek_columns(15) }},
            {{ gameweek_columns(16) }},
            {{ gameweek_columns(17) }},
            {{ gameweek_columns(18) }},
            {{ gameweek_columns(19) }},
            {{ gameweek_columns(20) }},
            {{ gameweek_columns(21) }},
            {{ gameweek_columns(22) }},
            {{ gameweek_columns(23) }},
            {{ gameweek_columns(24) }},
            {{ gameweek_columns(25) }},
            {{ gameweek_columns(26) }},
            {{ gameweek_columns(27) }},
            {{ gameweek_columns(28) }},
            {{ gameweek_columns(29) }},
            {{ gameweek_columns(30) }},
            {{ gameweek_columns(31) }},
            {{ gameweek_columns(32) }},
            {{ gameweek_columns(33) }},
            {{ gameweek_columns(34) }},
            {{ gameweek_columns(35) }},
            {{ gameweek_columns(36) }},
            {{ gameweek_columns(37) }},
            {{ gameweek_columns(38) }}
            
        from int_fixtures_teams_narrow_joined
        group by team order by team

),

final as (

    select * from fixture_id_pivot
)

select * from final


