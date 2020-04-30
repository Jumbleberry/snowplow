{{
    config(
        materialized='table',
        sort='first_session_start',
        dist='user_snowplow_domain_id',
        sql_where='first_session_start > (select max(first_session_start) from {{ this }})',
        unique_key='user_snowplow_domain_id'
    )
}}


with sessions as (

    select * from {{ ref('snowplow_sessions') }}

),

engagement as (

    select * from {{ ref('snowplow_engagement') }}

),

lead as (

    select * from {{ ref('snowplow_leads') }}

),


viewcontent as (

    select * from {{ ref('snowplow_viewcontent') }}

),


initiate_checkout as (

    select * from {{ ref('snowplow_initiate_checkout') }}

),

add_payment_info as (

    select * from {{ ref('snowplow_add_payment_info') }}

),

add_to_cart as (

    select * from {{ ref('snowplow_add_to_cart') }}

),

declines as (

    select * from {{ ref('snowplow_declines') }}
),

chargebacks as (

    select * from {{ ref('snowplow_chargebacks') }}

),

complete_registration as (

    select * from {{ ref('snowplow_complete_registration') }}

),

purchases as (

    select * from {{ ref('snowplow_purchases') }}

),

upsells as (

    select * from {{ ref('snowplow_upsells') }}

),

prep as (

    select
        inferred_user_id,

        min(session_start) as first_session_start,
        min(session_start_local) as first_session_start_local,
        max(session_end) as last_session_end,
        sum(page_views) as page_views,
        count(*) as sessions,
        sum(time_engaged_in_s) as time_engaged_in_s

    from sessions

    group by 1

),

users as (

    select
        -- user
        a.inferred_user_id,
        a.user_custom_id,
        a.user_snowplow_domain_id,
        a.user_snowplow_crossdomain_id,

        -- first sesssion: time
        b.first_session_start,

        -- first session: time in the user's local timezone
        b.first_session_start_local,

        -- last session: time
        b.last_session_end,

        -- engagement
        b.page_views,
        b.sessions,
        b.time_engaged_in_s,

        -- first page
        a.first_page_url,
        a.first_page_url_scheme,
        a.first_page_url_host,
        a.first_page_url_port,
        a.first_page_url_path,
        a.first_page_url_query,
        a.first_page_url_fragment,

        a.first_page_title,

        -- referer
        a.referer_url,
        a.referer_url_scheme,
        a.referer_url_host,
        a.referer_url_port,
        a.referer_url_path,
        a.referer_url_query,
        a.referer_url_fragment,

        a.referer_medium,
        a.referer_source,
        a.referer_term,

        -- marketing
        a.marketing_medium,
        a.marketing_source,
        a.marketing_term,
        a.marketing_content,
        a.marketing_campaign,
        a.marketing_click_id,
        a.marketing_network,

        -- application
        a.app_id,

        -- be extra cautious, ensure we only get one record per inferred_user_id
        row_number() over (partition by a.inferred_user_id order by a.session_start) as dedupe,
    
        -- events
        e.engagement_count,
        l.lead_count,
        v.viewcontent_count,
        ic.initiate_count,
        ap.add_payment_info_count,
        ac.add_to_cart_count,
        cr.complete_registration_count,
        

        -- declines
        d.decline_count,
    
        -- chargebacks
        cb.chargeback_count,
    
        -- purchases
        p.purchase_count,
    
        -- upsells
        u.upsell_count
    
    from sessions as a
        inner join prep as b on a.inferred_user_id = b.inferred_user_id
        left join engagement as e on a.inferred_user_id = e.inferred_user_id
        left join lead as l on a.inferred_user_id = l.inferred_user_id
        left join viewcontent as v on a.inferred_user_id = v.inferred_user_id
        left join initiate_checkout as ic on a.inferred_user_id = ic.inferred_user_id
        left join add_payment_info as ap on a.inferred_user_id = ap.inferred_user_id
        left join add_to_cart as ac on a.inferred_user_id = ac.inferred_user_id
        left join complete_registration as cr on a.inferred_user_id = cr.inferred_user_id
        left join declines as d on a.inferred_user_id = d.inferred_user_id
        left join chargebacks as cb on a.inferred_user_id = cb.inferred_user_id
        left join purchases as p on a.inferred_user_id = p.inferred_user_id
        left join upsells as u on a.inferred_user_id = u.inferred_user_id

    where a.session_index = 1
)

select * from users where dedupe = 1
