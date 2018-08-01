{{
    config(
        sort='event_id',
        dist='event_id',
        unique_key='event_id'
    )
}}

with event_to_user_map as (

    select 
        event_id,
        coalesce(user_id, domain_userid) as inferred_user_id
    from
        {{ ref('snowplow_base_events') }}

)

select * from event_to_user_map
