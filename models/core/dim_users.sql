{{ config(materialized='table') }}

select distinct
    "user_pseudo_id"
from {{ ref('fact_events') }}
where "user_pseudo_id" is not null