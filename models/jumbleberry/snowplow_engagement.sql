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
        count(e.*) as decline_count

    from event_to_user_map as m
        inner join engagement as e
        on m.event_id = e.event_id

    group by
        m.inferred_user_id
)

select * from engagement_with_user_id
