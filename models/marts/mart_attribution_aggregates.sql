-- models/marts/mart_attribution_aggregates.sql
{{ config(materialized='table') }}
select
    cast(purchase_ts as date) as dt,
    first_source as channel,
    count(*) as conversions_firstclick,
    sum(purchase_value) as revenue_firstclick,
    0 as conversions_lastclick,
    0 as revenue_lastclick
from {{ ref('first_click') }}
group by 1,2

union all

select
    cast(purchase_ts as date) as dt,
    last_source as channel,
    0 as conversions_firstclick,
    0 as revenue_firstclick,
    count(*) as conversions_lastclick,
    sum(purchase_value) as revenue_lastclick
from {{ ref('last_click') }}
group by 1,2