{{
    config(
        sort='event_id',
        dist='event_id',
        unique_key='event_id'
    )
}}

with submit_form as (

    select * from {{ var('snowplow:change_form') }}

)

select * from submit_form
