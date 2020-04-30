{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with add_to_cart as (

    select * from {{ var('snowplow:add_to_cart') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

add_to_cart_with_user_id as (

    select 
        m.inferred_user_id,
        count(ac.*) as add_to_cart_count
    from event_to_user_map as m
        inner join add_to_cart as ac
        on m.event_id = ac.event_id

    group by
        m.inferred_user_id
)

select * from add_to_cart_with_user_id
