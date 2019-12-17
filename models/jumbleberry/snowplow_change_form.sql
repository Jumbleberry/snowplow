{{
    config(
        sort='event_id',
        dist='event_id',
        unique_key='event_id'
    )
}}

with change_form as (

    select * from {{ var('snowplow:change_form') }}

)

select * from change_form
