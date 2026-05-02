{{ config(materialized='table') }}

select
    to_date("event_date",'YYYYMMDD') as event_day,
    count(*) as purchases

from {{ ref('fact_events') }}
where lower("event_name")='purchase'

group by 1
order by 1