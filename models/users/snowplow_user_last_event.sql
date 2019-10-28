{{
    config(
        materialized = 'table',
        sort='domain_userid',
        dist='domain_userid',
        unique_key='domain_userid'
    )
}}

with uevent as (
    select * from {{ ref('snowplow_users_last_ue')}}
),

prep as (
    select * from {{ ref('snowplow_last_user_page_view')}}
),

final as (
    SELECT 
    d.domain_userid,
    d.page_view_id,
    d.page_urlhost,
    d.page_urlpath,
    d.page_title,
    d.br_family,
    d.os_family,
    d.dvce_type,
    u.link_clicked_url,
    u.form_name,
    u.form_field_name,
    CASE
    WHEN u.domain_userid IS NOT NULL AND u.link_clicked_url IS NOT NULL THEN 'Link_Clicked'
    WHEN u.domain_userid IS NOT NULL AND u.form_name IS NOT NULL THEN 'Submitted_Form'
    WHEN u.domain_userid IS NOT NULL AND u.form_field_name IS NOT NULL THEN 'Filled_Form'
    ELSE 'Page_Viewed' END AS last_user_event
    FROM prep as d
    
    LEFT JOIN uevent as u
    on d.domain_userid = u.domain_userid
    
    GROUP BY 1,2,3,4,5,6,7,8,9
)

select * from final 
