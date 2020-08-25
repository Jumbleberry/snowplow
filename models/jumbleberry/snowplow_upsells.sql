{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with upsells as (

    select * from {{ var('snowplow:upsells') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

upsells_with_user_id as (

    select 
        m.inferred_user_id,
        count(u.*) as upsell_count,
        max(u.value) as upsell_max_value,
        sum(u.value) as upsell_total_value

    from event_to_user_map as m
        inner join upsells as u on m.event_id = u.event_id

    group by
        m.inferred_user_id
)

select * from upsells_with_user_id
