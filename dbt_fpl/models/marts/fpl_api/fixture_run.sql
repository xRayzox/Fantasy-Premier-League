with 

int_fixtures_teams_narrow_joined as (

    select * from {{ ref('int_fixtures_teams_narrow_joined') }}

),

final as (

{% set table = ref('int_fixture_pivot') %}

{% set columns = adapter.get_columns_in_relation(table) %}

select 

{% for column in columns %}

    {% if column.name != 'team' %}
    
        iftnj_{{ loop.index }}.opponent as {{ column.name }},
        iftnj_{{ loop.index }}.difficulty as {{ column.name }}_difficulty,
        iftnj_{{ loop.index }}.type as {{ column.name }}_type,

    {% endif %}

{% endfor %}

{{ table }}.team

from {{ table }}

{% for column in columns %}

        {% if column.name != 'team' %}

        left join int_fixtures_teams_narrow_joined as iftnj_{{ loop.index }}
        on iftnj_{{ loop.index }}.fixture_id = {{ table }}.{{ column.name }} and iftnj_{{ loop.index }}.team = {{ table }}.team
        
        {% endif %}

{% endfor %}

)

select * from final