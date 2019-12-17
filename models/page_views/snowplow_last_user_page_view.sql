{{
    config(
        materialized = 'view',
        sort='user_snowplow_domain_id',
        dist='user_snowplow_domain_id',
        unique_key='user_snowplow_domain_id'
    )
}}

with prep as (
    select 
    min_tstamp,
    user_snowplow_domain_id,
    affiliate_id,
    page_view_id,
    page_url_host,
    page_url_path,
    page_title,
    vertical_percentage_scrolled_tier,
    time_engaged_in_s_tier,
    time_engaged_in_s,
    browser_name,
    os_name,
    device_type,
    ROW_NUMBER() OVER (PARTITION BY user_snowplow_domain_id ORDER BY min_tstamp DESC) as de_dupe
    FROM {{ ref('snowplow_page_views') }}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from prep where de_dupe = '1'
