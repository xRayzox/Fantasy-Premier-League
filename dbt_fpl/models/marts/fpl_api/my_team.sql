with 

my_team as (

    select * from {{ ref('stg_fpl_api__my_team') }}

),

gameweek as (

    select * from {{ ref('stg_fpl_api__gameweek') }}

),

my_team_gameweek_joined as (

    select
        my_team.*,
        gameweek.gameweek_name,
        gameweek.average_score,
        gameweek.highest_score,
        gameweek.is_current
    
    from my_team

    left join gameweek
    on my_team.gameweek_id = gameweek.gameweek_id

),

final as (

    select

        gameweek_id,
        gameweek_name,
        average_score as gameweek_average_score,
        highest_score as gameweek_highest_score,
        is_current as is_current_gameweek,
        points,
        total_points,
        rank,
        rank_sort,
        overall_rank,
        bank,
        value as team_value,
        gameweek_transfer_count,
        gameweek_transfers_point_cost,
        points_on_bench

    from my_team_gameweek_joined

)

select * from final