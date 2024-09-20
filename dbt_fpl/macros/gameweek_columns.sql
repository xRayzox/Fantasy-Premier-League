{% macro gameweek_columns(id) %}

    {% set fixture_id = 'Min( Case gameweek_id when ' ~ id ~ ' then fixture_id End )' %}

    {% set max_fixture_id = 'Max( Case gameweek_id when ' ~ id ~ ' then fixture_id End )' %}

    {% set double_gameweek_fixture_id = 'Case ' ~ fixture_id ~ ' when ' ~ max_fixture_id ~ ' then NULL else ' ~ max_fixture_id ~ ' End' %}

    {% set gameweek_id = 'Min( Case gameweek_id when ' ~ id ~ ' then opponent End )' %}

    {% set max_gameweek_id =  'Max( Case gameweek_id when ' ~ id ~ ' then opponent End )' %}

    {% set double_gameweek_id = 'Case ' ~ gameweek_id ~ ' when ' ~ max_gameweek_id ~ ' then NULL else ' ~ max_gameweek_id ~ ' End' %}

    {% set gameweek_difficulty = 'Round( Avg( Case gameweek_id when ' ~ id ~ ' then difficulty End ), 2 )' %}

    {{ fixture_id }} as gameweek_{{ id }},
    {{ double_gameweek_fixture_id }} as double_gameweek_{{ id }}

{% endmacro %}
