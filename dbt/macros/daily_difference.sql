{#
This macro calculates the difference between the current day’s total vaccinations and the previous day’s total.
#}


{% macro daily_difference(column_name) %}
    {{ column_name }} - LAG({{ column_name }}) OVER (PARTITION BY location ORDER BY vaccination_date) 
{% endmacro %}