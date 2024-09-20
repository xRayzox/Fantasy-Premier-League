with

source as (

    select * from {{ source('fpl_api','teams') }}

),

teams as (

    select

        id as team_id,
        name,
        short_name,
        strength,
        strength_overall_home,
        strength_overall_away,
        strength_attack_home,
        strength_attack_away,
        strength_defence_home,
        strength_defence_away
    
    from source
),

final as (

    select * from teams

)

select * from final