{{
    config(
        materialized = 'table',
        sort='domain_userid',
        dist='domain_userid',
        unique_key='domain_userid'
    )
}}

with events as (
    select * from {{ var('snowplow:events') }}
),

link_click as (
    select * from {{ ref('snowplow_link_click') }}
),

submit_form as (
    select * from {{ ref('snowplow_submit_form') }}
),

change_form as (
    select * from {{ ref('snowplow_change_form') }}
),


prep as (
    SELECT
    ev.event,
    ev.event_id,
    ev.domain_userid,
    ev.collector_tstamp,
    lc.target_url as link_clicked_url,
    sf.form_id as form_name,
    cf.element_id as form_field_name,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY ev.domain_userid ORDER BY ev.collector_tstamp DESC) = '1' AND EVENT = 'ue' THEN '1' ELSE '0' END as de_dupe
    FROM events as ev
    
    LEFT JOIN link_click as lc
    ON ev.event_id = lc.event_id
    
    LEFT JOIN submit_form as sf
    ON ev.event_id = sf.event_id
    
    LEFT JOIN change_form as cf
    ON ev.event_id = sf.event_id
    
    GROUP BY 1,2,3,4,5,6,7
),

de_dupe as (
    SELECT 
    domain_userid,
    link_clicked_url,
    form_name,
    form_field_name
    FROM prep
    WHERE de_dupe = '1'
)

select * from de_dupe

