{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with complete_registration as (

    select * from {{ var('snowplow:complete_registration') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

complete_registration_with_user_id as (

    select 
        m.inferred_user_id,
        count(cr.*) as complete_registrations
        
    from event_to_user_map as m
        inner join complete_registration as cr 
        on m.event_id = cr.event_id

    group by
        m.inferred_user_id
)

select * from complete_registration_with_user_id
