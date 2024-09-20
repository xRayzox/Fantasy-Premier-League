with 

my_team_transfers as (

    select * from {{ ref('stg_fpl_api__my_team_transfers') }}

),

players as (

    select * from {{ ref('stg_fpl_api__players') }}

),

int_gameweek_players_joined as (

    select * from {{ ref('int_gameweek_players_joined') }}

),

my_team_transfers_players_joined as(

    select 

        my_team_transfers.id,
        pi.web_name as player_in,
        my_team_transfers.player_in_cost,
        po.web_name as player_out,
        my_team_transfers.player_out_cost,
        my_team_transfers.gameweek_id,
        my_team_transfers.timestamp

    from my_team_transfers

    left join players as pi
    on my_team_transfers.player_in_id = pi.player_id

    left join players as po
    on my_team_transfers.player_out_id = po.player_id

),

my_team_transfers_gameweek_joined as (

    select
        my_team_transfers_players_joined.*,
        int_gameweek_players_joined.gameweek_name,
        int_gameweek_players_joined.is_current,
        int_gameweek_players_joined.most_transferred_in
    
    from my_team_transfers_players_joined

    left join int_gameweek_players_joined
    on my_team_transfers_players_joined.gameweek_id = int_gameweek_players_joined.gameweek_id

),

final as (

    select
        
        id,
        player_in,
        player_in_cost,
        player_out,
        player_out_cost,
        gameweek_id
        gameweek_name,
        is_current as is_current_gameweek,
        most_transferred_in,
        timestamp

    from my_team_transfers_gameweek_joined

)

select * from final