-- models/int/int_events_flat.sql
{{ config(materialized='table') }}
with events as (

    select * from {{ ref('stg_ga4_events') }}

),

dedup as (

    select
        *,
        row_number() over (
            partition by event_id
            order by event_ts desc
        ) as rn
    from events

)

select
    event_date,
    event_ts,
    user_pseudo_id,
    event_name,
    event_id,
    page_location,
    item_id,
    purchase_value,
    traffic_source_source,
    traffic_source_medium,
    traffic_source_campaign
from dedup
where rn = 1