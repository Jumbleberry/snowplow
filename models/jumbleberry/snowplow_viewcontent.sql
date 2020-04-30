{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with viewcontent as (

    select * from {{ var('snowplow:viewcontent') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

viewcontent_with_user_id as (

    select 
        m.inferred_user_id,
        count(d.*) as viewcontent_count
        
    from event_to_user_map as m
        inner join viewcontent as v on m.event_id = v.event_id

    group by
        m.inferred_user_id
)

select * from viewcontent_with_user_id
