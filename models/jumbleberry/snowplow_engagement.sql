{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with engagement as (

    select * from {{ var('snowplow:engagement') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

engagement_with_user_id as (

    select 
        m.inferred_user_id,
        max(e.c_1) as c_1,
        max(e.c_2) as c_2,
        max(e.c_3) as c_3

    from event_to_user_map as m
        inner join engagement as e on m.event_id = e.event_id

    group by
        m.inferred_user_id
)

select * from engagement_with_user_id
