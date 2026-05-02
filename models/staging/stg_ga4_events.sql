-- models/staging/stg_ga4_events.sql
{{ config(materialized='table') }}
select * from {{ ref('stg_events') }}