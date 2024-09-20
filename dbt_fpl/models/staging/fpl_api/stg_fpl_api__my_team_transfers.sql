with

source as (

    select * from {{ source('fpl_api','my_team_transfers') }}

),

my_team_transfers as (

    select

        id,
		player_in as player_in_id,
		player_in_cost/10::float as player_in_cost,
		player_out as player_out_id,
		player_out_cost/10::float as player_out_cost,
		gameweek as gameweek_id,
		time as timestamp

    from source
),

final as (

    select * from my_team_transfers

)

select * from final