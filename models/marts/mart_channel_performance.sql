{{ config(materialized='view') }}

{% set tables = dbt_utils.get_relations_by_pattern(
    schema_pattern='RAW',
    table_pattern='EVENTS_%'
) %}

with unioned as (

{% for table in tables %}

select
    *,
    '{{ table.identifier }}' as source_table
from {{ table }}

{% if not loop.last %}
union all
{% endif %}

{% endfor %}

),

final as (

select
    to_date(event_date, 'YYYYMMDD') as event_date,
    to_timestamp_ntz(event_timestamp / 1000000) as event_ts,

    user_pseudo_id,
    event_name,
    event_id,
    event_previous_timestamp,

    source_table,

    traffic_source:source::string   as traffic_source_source,
    traffic_source:medium::string   as traffic_source_medium,
    traffic_source:campaign::string as traffic_source_campaign,

    (
      select f.value:value.string_value::string
      from lateral flatten(input => event_params) f
      where f.value:key::string = 'page_location'
      limit 1
    ) as page_location,

    (
      select f.value:value.string_value::string
      from lateral flatten(input => event_params) f
      where f.value:key::string = 'item_id'
      limit 1
    ) as item_id,

    (
      select f.value:value.double_value::float
      from lateral flatten(input => event_params) f
      where f.value:key::string = 'value'
      limit 1
    ) as purchase_value

from unioned

)

select * from final