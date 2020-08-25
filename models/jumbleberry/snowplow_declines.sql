{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with declines as (

    select * from {{ var('snowplow:declines') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

declines_with_user_id as (

    select 
        m.inferred_user_id,
        count(d.*) as decline_count,
        sum(d.value) as decline_total_value

    from event_to_user_map as m
        inner join declines as d on m.event_id = d.event_id

    group by
        m.inferred_user_id
)

select * from declines_with_user_id
