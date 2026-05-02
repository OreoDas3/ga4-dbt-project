{{ config(materialized='table') }}

with purchases as (

    select
        event_id as purchase_event_id,
        user_pseudo_id,
        event_ts as purchase_ts,
        purchase_value
    from {{ ref('int_events_flat') }}
    where event_name = 'purchase'

),

touchpoints as (

    select
        event_id as touch_event_id,
        user_pseudo_id,
        event_ts as touch_ts,
        traffic_source_source,
        traffic_source_medium,
        traffic_source_campaign
    from {{ ref('int_events_flat') }}
    where event_name <> 'purchase'

),

joined as (

    select
        p.purchase_event_id,
        p.user_pseudo_id,
        p.purchase_ts,
        p.purchase_value,

        t.touch_event_id,
        t.touch_ts,
        t.traffic_source_source,
        t.traffic_source_medium,
        t.traffic_source_campaign,

        row_number() over (
            partition by p.purchase_event_id
            order by t.touch_ts desc nulls last
        ) as rn

    from purchases p
    left join touchpoints t
      on p.user_pseudo_id = t.user_pseudo_id
     and t.touch_ts < p.purchase_ts
     and t.touch_ts >= dateadd(day, -30, p.purchase_ts)

)

select
    purchase_event_id,
    user_pseudo_id,
    purchase_ts,
    purchase_value,
    coalesce(traffic_source_source, '(direct)') as last_source,
    coalesce(traffic_source_medium, '(none)') as last_medium,
    coalesce(traffic_source_campaign, '(none)') as last_campaign
from joined
where rn = 1