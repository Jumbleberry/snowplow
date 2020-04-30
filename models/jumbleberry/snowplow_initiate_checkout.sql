{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with initiate_checkout as (

    select * from {{ var('snowplow:initiate_checkout') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

initiate_checkout_with_user_id as (

    select 
        m.inferred_user_id,
        count(ic.*) as initiate_checkout_count

    from event_to_user_map as m
        inner join initiate_checkout as ic
        on m.event_id = ic.event_id

    group by
        m.inferred_user_id
)

select * from initiate_checkout_with_user_id
