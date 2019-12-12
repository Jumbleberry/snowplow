{{
    config(
        sort='event_id',
        dist='event_id',
        unique_key='event_id'
    )
}}

with link_click as (

    select * from {{ var('snowplow:link_click') }}

)

select 
event_id,
target_url
from link_click
