{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

with chargebacks as (

    select * from {{ var('snowplow:chargebacks') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

chargebacks_with_user_id as (

    select 
        m.inferred_user_id,
        count(c.*) as chargeback_count,
        sum(c.value) as chargeback_total_value

    from event_to_user_map as m
        inner join chargebacks as c on m.event_id = c.event_id

    group by
        m.inferred_user_id
)

select * from chargebacks_with_user_id
