{#
This macro checks the column type and fills NULL values with defaults:

Numeric columns → Replace NULL with 0

Percentage columns → Replace NULL with 0.0

Date columns → Replace NULL with '1970-01-01'

String columns → Replace NULL with 'Unknown'
#}


{% macro fill_nulls(column_name, column_type) %}
    {% if column_type in ('integer', 'float', 'numeric', 'bigint', 'double') %}
        COALESCE({{ column_name }}, 0) AS {{ column_name }}
    {% elif column_type in ('varchar', 'string', 'text') %}
        COALESCE({{ column_name }}, 'Unknown') AS {{ column_name }}
    {% elif column_type in ('date', 'timestamp', 'datetime') %}
        COALESCE({{ column_name }}, '1970-01-01') AS {{ column_name }}
    {% else %}
        {{ column_name }}  -- Keep it unchanged if type is unknown
    {% endif %}
{% endmacro %}
