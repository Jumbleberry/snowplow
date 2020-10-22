
{{
    config(
        materialized='incremental',
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
        sum(time_engaged_in_s) as time_engaged_in_s,
        max(viewports_height) as viewports_height

    from sessions

    group by 1

),

users as (

    select
        -- user
        a.inferred_user_id,
        a.user_snowplow_domain_id,

        -- first sesssion: time
        b.first_session_start,

        -- derived dimensions
        to_char(b.first_session_start, 'YYYY-MM-DD HH24:MI:SS') as first_session_time,
        to_char(b.first_session_start, 'YYYY-MM-DD HH24:MI') as first_session_minute,
        to_char(b.first_session_start, 'YYYY-MM-DD HH24') as first_session_hour,
        to_char(b.first_session_start, 'YYYY-MM-DD') as first_session_date,
        to_char(date_trunc('week', b.first_session_start), 'YYYY-MM-DD') as first_session_week,
        to_char(b.first_session_start, 'YYYY-MM') as first_session_month,
        to_char(date_trunc('quarter', b.first_session_start), 'YYYY-MM') as first_session_quarter,
        date_part('y', b.first_session_start)::integer as first_session_year,

        -- first session: time in the user's local timezone
        b.first_session_start_local,

        -- derived local dimensions
        to_char(b.first_session_start_local, 'YYYY-MM-DD HH24:MI:SS') as first_session_local_time,
        to_char(b.first_session_start_local, 'HH24:MI') as first_session_local_time_of_day,
        date_part('hour', b.first_session_start_local)::integer as first_session_local_hour_of_day,
        trim(to_char(b.first_session_start_local, 'd')) as first_session_local_day_of_week,
        mod(extract(dow from b.first_session_start_local)::integer - 1 + 7, 7) as first_session_local_day_of_week_index,

        -- last session: time
        b.last_session_end,
        to_char(b.last_session_end, 'YYYY-MM-DD HH24:MI:SS') as last_session_time,
        EXTRACT(EPOCH FROM (b.last_session_end - b.first_session_start)) as time_elapsed_in_s,

        -- session
        b.sessions,
        b.time_engaged_in_s,

        (
          COALESCE(engagement_pv_count, 0) +
          COALESCE(lead_pv_count, 0) +
          COALESCE(viewcontent_pv_count, 0) +
          COALESCE(initiate_checkout_pv_count, 0) +
          COALESCE(add_payment_info_pv_count, 0) +
          COALESCE(add_to_cart_pv_count, 0) +
          COALESCE(declines_pv_count, 0) +
          COALESCE(chargebacks_pv_count, 0) +
          COALESCE(complete_registration_pv_count, 0) +
          COALESCE(purchases_pv_count, 0) +
          COALESCE(upsells_pv_count, 0)
        ) AS page_views,

        (
          COALESCE(engagement_pp_count, 0) +
          COALESCE(lead_pp_count, 0) +
          COALESCE(viewcontent_pp_count, 0) +
          COALESCE(initiate_checkout_pp_count, 0) +
          COALESCE(add_payment_info_pp_count, 0) +
          COALESCE(add_to_cart_pp_count, 0) +
          COALESCE(declines_pp_count, 0) +
          COALESCE(chargebacks_pp_count, 0) +
          COALESCE(complete_registration_pp_count, 0) +
          COALESCE(purchases_pp_count, 0) +
          COALESCE(upsells_pp_count, 0)
        ) AS page_pings,

        (
          COALESCE(engagement_vertical_pixels_scrolled, 0) +
          COALESCE(lead_vertical_pixels_scrolled, 0) +
          COALESCE(viewcontent_vertical_pixels_scrolled, 0) +
          COALESCE(initiate_checkout_vertical_pixels_scrolled, 0) +
          COALESCE(add_payment_info_vertical_pixels_scrolled, 0) +
          COALESCE(add_to_cart_vertical_pixels_scrolled, 0) +
          COALESCE(declines_vertical_pixels_scrolled, 0) +
          COALESCE(chargebacks_vertical_pixels_scrolled, 0) +
          COALESCE(complete_registration_vertical_pixels_scrolled, 0) +
          COALESCE(purchases_vertical_pixels_scrolled, 0) +
          COALESCE(upsells_vertical_pixels_scrolled, 0)
        ) AS vertical_pixels_scrolled,

        (
          COALESCE(engagement_viewports_consumed, 0) +
          COALESCE(lead_viewports_consumed, 0) +
          COALESCE(viewcontent_viewports_consumed, 0) +
          COALESCE(initiate_checkout_viewports_consumed, 0) +
          COALESCE(add_payment_info_viewports_consumed, 0) +
          COALESCE(add_to_cart_viewports_consumed, 0) +
          COALESCE(declines_viewports_consumed, 0) +
          COALESCE(chargebacks_viewports_consumed, 0) +
          COALESCE(complete_registration_viewports_consumed, 0) +
          COALESCE(purchases_viewports_consumed, 0) +
          COALESCE(upsells_viewports_consumed, 0)
        ) AS viewports_consumed,

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

        -- bridge page engagement
        e.c_1,
        e.c_2,
        e.c_3,
        e.engagement_count,
        e.engagement_pv_count,
        e.engagement_pp_count,
        e.engagement_time_engaged_in_s,
        e.engagement_first_time,
        e.engagement_vertical_pixels_scrolled,
        e.engagement_viewport_length,
        e.engagement_viewports_consumed,

        -- leads
        l.lead_count,
        l.lead_pv_count,
        l.lead_pp_count,
        l.lead_time_engaged_in_s,
        l.lead_first_time,
        l.lead_vertical_pixels_scrolled,
        l.lead_viewport_length,
        l.lead_viewports_consumed,

        -- viewcontent
        v.viewcontent_count,
        v.viewcontent_pv_count,
        v.viewcontent_pp_count,
        v.viewcontent_time_engaged_in_s,
        v.viewcontent_first_time,
        v.viewcontent_vertical_pixels_scrolled,
        v.viewcontent_viewport_length,
        v.viewcontent_viewports_consumed,

        -- initiate_checkout
        ic.initiate_checkout_count,
        ic.initiate_checkout_pv_count,
        ic.initiate_checkout_pp_count,
        ic.initiate_checkout_time_engaged_in_s,
        ic.initiate_checkout_first_time,
        ic.initiate_checkout_vertical_pixels_scrolled,
        ic.initiate_checkout_viewport_length,
        ic.initiate_checkout_viewports_consumed,

        -- add_payment_info
        ap.add_payment_info_count,
        ap.add_payment_info_pv_count,
        ap.add_payment_info_pp_count,
        ap.add_payment_info_time_engaged_in_s,
        ap.add_payment_info_first_time,
        ap.add_payment_info_vertical_pixels_scrolled,
        ap.add_payment_info_viewport_length,
        ap.add_payment_info_viewports_consumed,

        -- add_to_cart
        ac.add_to_cart_count,
        ac.add_to_cart_pv_count,
        ac.add_to_cart_pp_count,
        ac.add_to_cart_time_engaged_in_s,
        ac.add_to_cart_first_time,
        ac.add_to_cart_vertical_pixels_scrolled,
        ac.add_to_cart_viewport_length,
        ac.add_to_cart_viewports_consumed,

        -- complete_registration
        cr.complete_registration_count,
        cr.complete_registration_pv_count,
        cr.complete_registration_pp_count,
        cr.complete_registration_time_engaged_in_s,
        cr.complete_registration_first_time,
        cr.complete_registration_vertical_pixels_scrolled,
        cr.complete_registration_viewport_length,
        cr.complete_registration_viewports_consumed,

        -- declines
        d.declines_count,
        d.declines_total_value,
        d.declines_pv_count,
        d.declines_pp_count,
        d.declines_time_engaged_in_s,
        d.declines_first_time,
        d.declines_vertical_pixels_scrolled,
        d.declines_viewport_length,
        d.declines_viewports_consumed,

        -- chargebacks
        cb.chargeback_total_value,
        cb.chargebacks_count,
        cb.chargebacks_pv_count,
        cb.chargebacks_pp_count,
        cb.chargebacks_time_engaged_in_s,
        cb.chargebacks_first_time,
        cb.chargebacks_vertical_pixels_scrolled,
        cb.chargebacks_viewport_length,
        cb.chargebacks_viewports_consumed,

        -- purchases
        p.purchases_max_value,
        p.purchases_total_value,
        p.purchases_count,
        p.purchases_pv_count,
        p.purchases_pp_count,
        p.purchases_time_engaged_in_s,
        p.purchases_first_time,
        p.purchases_vertical_pixels_scrolled,
        p.purchases_viewport_length,
        p.purchases_viewports_consumed,

        -- upsells
        u.upsells_max_value,
        u.upsells_total_value,
        u.upsells_count,
        u.upsells_pv_count,
        u.upsells_pp_count,
        u.upsells_time_engaged_in_s,
        u.upsells_first_time,
        u.upsells_vertical_pixels_scrolled,
        u.upsells_viewport_length,
        u.upsells_viewports_consumed,

        GETDATE() AS updated

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

{% if is_incremental() %}

  and last_session_end > (select max(last_session_end) from {{ this }})

{% endif %}