{{
    config(
        materialized = 'ephemeral',
        sort='domain_userid',
        dist='domain_userid',
        unique_key='domain_userid'
    )
}}

with prep as (
    select 
    collector_tstamp,
    domain_userid,
    page_view_id,
    page_urlhost,
    page_urlpath,
    page_title,
    br_family,
    os_family,
    dvce_type,
    ROW_NUMBER() OVER (PARTITION BY domain_userid ORDER BY collector_tstamp DESC) as de_dupe
    FROM {{ ref('snowplow_web_events') }}
    GROUP BY 1,2,3,4,5,6,7,8,9
)

select * from prep where de_dupe = '1'
