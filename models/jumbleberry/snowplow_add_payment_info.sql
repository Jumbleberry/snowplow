{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

{% set timezone = var('snowplow:timezone', 'UTC') %}

with add_payment_info as (

    select * from {{ var('snowplow:add_payment_info') }}

),

event_to_user_map as (

    select * from {{ ref('snowplow_event_to_user_map') }}

),

web_page_context as (

    select event_id, page_view_id from {{ ref('snowplow_web_page_context') }}

),

web_events_time as (
  select * from {{ ref('snowplow_web_events_time') }}
),

web_events_scroll_depth as (

    select * from {{ ref('snowplow_web_events_scroll_depth') }}

),

add_payment_info_with_user_id as (

    select 
        m.inferred_user_id,
        count(e.*) as add_payment_info_count,
        SUM(t.pv_count) AS add_payment_info_pv_count,
        SUM(t.pp_count) AS add_payment_info_pp_count,
        SUM(t.time_engaged_in_s) AS add_payment_info_time_engaged_in_s,
        {{convert_timezone("'UTC'", "'" ~ timezone ~ "'", 'MIN(t.min_tstamp)')}} as add_payment_info_first_time,
        MAX(d.vmax) AS add_payment_info_vertical_pixels_scrolled,
        MAX(d.br_viewheight) AS add_payment_info_viewport_length,
        round(add_payment_info_vertical_pixels_scrolled / NULLIF(add_payment_info_viewport_length, 0), 2) + 1 AS add_payment_info_viewports_consumed
        
    from event_to_user_map as m
        inner join add_payment_info as e on m.event_id = e.event_id
        left join web_page_context as c on e.event_id = c.event_id
        left join web_events_time as t on c.page_view_id = t.page_view_id
        left join web_events_scroll_depth as d on c.page_view_id = d.page_view_id

    group by
        m.inferred_user_id
)

select * from add_payment_info_with_user_id
