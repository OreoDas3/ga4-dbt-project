{{ config(materialized='table') }}

select
    "device":"category"::string as device_type,
    count(*) as total_events,
    count(distinct "user_pseudo_id") as users

from {{ ref('fact_events') }}

group by 1
order by total_events desc