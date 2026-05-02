{{ config(materialized='table') }}

with dates as (

    select dateadd(day, seq4(), '2021-01-01') as date_day
    from table(generator(rowcount => 2000))

)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,
    week(date_day) as week_number,
    day(date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    dayname(date_day) as day_name,
    case when dayofweek(date_day) in (0,6) then true else false end as is_weekend
from dates