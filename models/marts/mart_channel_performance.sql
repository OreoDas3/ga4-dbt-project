{{ config(materialized='table') }}

select
    "traffic_source":"source"::string as source,
    "traffic_source":"medium"::string as medium,
    count(*) as total_events,
    count(distinct "user_pseudo_id") as users

from {{ ref('fact_events') }}

group by 1,2
order by total_events desc