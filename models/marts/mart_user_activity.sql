{{ config(materialized='table') }}

select
    "user_pseudo_id",
    count(*) as total_events

from {{ ref('fact_events') }}

group by 1
order by total_events desc
limit 100