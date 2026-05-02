{{ config(materialized='view') }}

{% set tables = dbt_utils.get_relations_by_pattern(
    schema_pattern='RAW',
    table_pattern='EVENTS_%'
) %}

{% for table in tables %}

select *,
       '{{ table.identifier }}' as source_table
from {{ table }}

{% if not loop.last %}
union all
{% endif %}

{% endfor %}