with

gameweek_players_joined as (

    select * from {{ ref('int_gameweek_players_joined') }}

),

gameweek_info as (

    select 
    
        *,
        (   case 
                when is_previous = true then 'previous'
                when is_current = true then 'current'
                when is_next = true then 'next'
                else null
            end ) as status

    from gameweek_players_joined
        
    where
        
        is_previous = true or is_current = true or is_next = true

),

final as (

    select

        gameweek_id,
        gameweek_name,
        deadline_time,
        average_score,
        finished,
        data_checked,
        highest_score,
        most_selected,
        most_transferred_in,
        most_scoring,
        transfers_made,
        most_captained,
        most_vice_captained,
        benchboost,
        triple_captain,
        wildcard,
        freehit,
        most_scoring_player_points,
        status

    from gameweek_info

)

select * from final