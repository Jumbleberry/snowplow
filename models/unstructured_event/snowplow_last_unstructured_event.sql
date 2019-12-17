{{
    config(
        materialized = 'view',
        sort='domain_userid',
        dist='domain_userid',
        unique_key='domain_userid'
    )
}}

with prep as (
    SELECT
    ev.event,
    ev.event_id,
    ev.domain_userid,
    ev.collector_tstamp,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY ev.domain_userid ORDER BY ev.collector_tstamp DESC) = '1' AND EVENT = 'ue' THEN '1' ELSE '0' END as de_dupe
    FROM {{ ref('snowplow_base_events') }} as ev
    GROUP BY 1,2,3,4
),

joined as (

    select 
    p.domain_userid,
    lc.target_url as link_clicked_url,
    sf.form_id as form_name,
    cf.element_id as form_field_name
    from prep p
    
    left join {{ ref('snowplow_link_click') }} as lc
    on p.event_id = lc.event_id
    left join {{ ref('snowplow_submit_form') }} as sf
    on p.event_id = sf.event_id
    left join {{ ref('snowplow_change_form') }} as cf
    on p.event_id = cf.event_id
    
    WHERE p.de_dupe = '1'
)

select * from joined


