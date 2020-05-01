{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with add_payment_info as (

    select * from {{ var('snowplow:add_payment_info') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

add_payment_info_with_user_id as (

    select 
        m.inferred_user_id,
        count(ap.*) as add_payment_info_count
        
    from event_to_user_map as m
        inner join add_payment_info as ap
        on m.event_id = ap.event_id

    group by
        m.inferred_user_id
)

select * from add_payment_info_with_user_id
