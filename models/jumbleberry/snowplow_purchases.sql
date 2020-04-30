{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with purchases as (

    select * from {{ var('snowplow:purchases') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

purchases_with_user_id as (

    select 
        m.inferred_user_id,
        count(p.*) as purchase_count,
        max(p.value) as purchase_max_value,
        sum(p.value) as purchase_total_value

    from event_to_user_map as m
        inner join purchases as p on m.event_id = p.event_id

    group by
        m.inferred_user_id
)

select * from purchases_with_user_id
