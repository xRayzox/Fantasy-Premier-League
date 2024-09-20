with

players as (

    select * from {{ ref('stg_fpl_api__players') }}

),

players_ranked as (

    select
        *,
        rank() OVER (PARTITION BY team_id ORDER BY total_points DESC)
    
    from players

),

final as (

    select * from players_ranked
    where rank <=3

)

select * from final