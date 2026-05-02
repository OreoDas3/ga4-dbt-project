{{ config(materialized='table') }}

select
    d.date_day,
    d.day_name,
    d.month_name,

    count(*) as total_events,
    count(distinct f."user_pseudo_id") as total_users

from {{ ref('fact_events') }} f
join {{ ref('dim_dates') }} d
    on to_date(f."event_date") = d.date_day

group by 1,2,3
order by 1