{{
    config(
        materialized = 'table',
        sort='user_snowplow_domain_id',
        dist='user_snowplow_domain_id',
        unique_key='user_snowplow_domain_id'
    )
}}

with uevent as (
    select * from {{ ref('snowplow_last_unstructured_event')}}
),

prep as (
    select * from {{ ref('snowplow_last_user_page_view')}}
),

final as (
    SELECT 
    d.min_tstamp,
    d.user_snowplow_domain_id,
    d.affiliate_id,
    d.page_view_id,
    d.page_url_host,
    d.page_url_path,
    d.page_title,
    d.vertical_percentage_scrolled_tier,
    d.time_engaged_in_s_tier,
    d.time_engaged_in_s,
    d.browser_name,
    d.os_name,
    d.device_type,
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
    on d.user_snowplow_domain_id = u.domain_userid
    
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

select * from final 
