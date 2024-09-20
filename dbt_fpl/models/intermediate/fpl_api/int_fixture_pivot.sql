with 

int_fixture_id_pivot as (

    select * from {{ ref('int_fixture_id_pivot') }}

),

final as (

{% set table = ref('int_fixture_id_pivot') %}

{% set columns = adapter.get_columns_in_relation(table) %}

select 

{% for column in columns %}

    {% set sql_statement %}

        select count({{ column.name }}) from {{ table }} where {{ column.name }} is not null

    {% endset %}

    {% set count = dbt_utils.get_single_value(sql_statement) %}

    {% if count != 0 and column.name != 'team' %}

        {{ column.name }},

    {% endif %}

{% endfor %}

team

from int_fixture_id_pivot

)

select * from final

