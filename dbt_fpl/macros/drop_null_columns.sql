{% macro drop_null_columns(column,table) %}

    {% set sql_statement %}

        select count({{ column }}) from {{ table }} where {{ column }} is not null

    {% endset %}

    {% set count = dbt_utils.get_single_value(sql_statement) %}

    {% if count == 0 %}

        {% set query %}
            alter table {{ table }} drop column {{ column }}
        {% endset %}
        {% do run_query(query) %}

    {% endif %}

{% endmacro %}