with

source as (

    select * from {{ source('fpl_api','my_team') }}

),

my_team as (

    select

        id,
		gameweek as gameweek_id,
		points,
		total_points,
		rank,
		rank_sort,
		overall_rank,
		bank/10::float as bank,
		value/10::float as value,
		gameweek_transfers as gameweek_transfer_count,
		gameweek_transfers_point_cost,
		points_on_bench

    from source
),

final as (

    select * from my_team

)

select * from final