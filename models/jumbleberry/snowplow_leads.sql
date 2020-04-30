{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with leads as (

    select * from {{ var('snowplow:lead') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

leads_with_user_id as (

    select 
        m.inferred_user_id,
        count(l.*) as lead_count
        
    from event_to_user_map as m
        inner join leads as l 
        on m.event_id = l.event_id

    group by
        m.inferred_user_id
)

select * from leads_with_user_id
