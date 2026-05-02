{{ config(materialized='table') }}

select
    "geo":"country"::string as country,
    count(*) as total_events,
    count(distinct "user_pseudo_id") as users

from {{ ref('fact_events') }}

group by 1
order by total_events desc