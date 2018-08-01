{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with metadata as (

    select * from {{ var('snowplow:metadata') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

metadata_with_user_id as (

    select 
        m.inferred_user_id,
        max(md.hit_id) as hit_id,
        max(md.trans_id) as trans_id,
        max(md.campaign_id) as campaign_id,
        max(md.c_1) as c_1,
        max(md.c_2) as c_2,
        max(md.c_3) as c_3

    from event_to_user_map as m
        inner join metadata as md on m.event_id = md.event_id

    group by
        m.inferred_user_id
)

select * from metadata_with_user_id
