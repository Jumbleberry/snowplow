{{
    config(
        sort='event_id',
        dist='event_id',
        unique_key='event_id'
    )
}}

with sumbit_form as (

    select * from {{ var('snowplow:submit_form') }}

)

select 
event_id,
form_id
from sumbit_form
