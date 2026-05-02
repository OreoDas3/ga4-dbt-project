{{ config(materialized='table') }}

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

flattened as (

select
    u.*,
    f.value:key::string as param_key,
    f.value:value.string_value::string as string_val,
    f.value:value.double_value::float as double_val
from unioned u,
lateral flatten(input => u."event_params") f

),

final as (

select
    to_date("event_date",'YYYYMMDD') as event_date,
    to_timestamp_ntz("event_timestamp"/1000000) as event_ts,

    "user_pseudo_id" as user_pseudo_id,
    "event_name" as event_name,

    md5(
        coalesce("user_pseudo_id",'') ||
        coalesce("event_name",'') ||
        coalesce("event_timestamp"::string,'')
    ) as event_id,

    "event_previous_timestamp" as event_previous_timestamp,
    source_table,

    "traffic_source":source::string as traffic_source_source,
    "traffic_source":medium::string as traffic_source_medium,
    "traffic_source":campaign::string as traffic_source_campaign,

    max(case when param_key='page_location' then string_val end) as page_location,
    max(case when param_key='item_id' then string_val end) as item_id,
    max(case when param_key='value' then double_val end) as purchase_value

from flattened

group by
    "event_date",
    "event_timestamp",
    "user_pseudo_id",
    "event_name",
    "event_previous_timestamp",
    source_table,
    "traffic_source"

)

select * from final