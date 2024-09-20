with

source as (

    select * from {{ source('fpl_api','gameweek') }}

),

gameweek as (

    select

        id as gameweek_id,
        name as gameweek_name,
        deadline_time,
        average_entry_score as average_score,
        finished,
        data_checked,
        highest_scoring_entry as highest_scoring_manager,
        highest_score,
        is_previous,
        is_current,
        is_next,
        most_selected as most_selected_player_id,
        most_transferred_in as most_transferred_in_player_id,
        most_scoring_player as most_scoring_player_id,
        transfers_made,
        most_captained as most_captained_player_id,
        most_vice_captained as most_vice_captained_player_id,
        benchboost,
        triple_captain,
        wildcard,
        freehit,
        most_scoring_player_points

    from source
),

final as (

    select * from gameweek

)

select * from final