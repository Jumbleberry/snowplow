{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

{% set timezone = var('snowplow:timezone', 'UTC') %}

with add_to_cart as (

    select * from {{ var('snowplow:add_to_cart') }}

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

add_to_cart_with_user_id as (

    select 
        m.inferred_user_id,
        count(e.*) as add_to_cart_count,
        SUM(t.pv_count) AS add_to_cart_pv_count,
        SUM(t.pp_count) AS add_to_cart_pp_count,
        SUM(t.time_engaged_in_s) AS add_to_cart_time_engaged_in_s,
        {{convert_timezone("'UTC'", "'" ~ timezone ~ "'", 'MIN(t.min_tstamp)')}} as add_to_cart_first_time,
        MAX(d.vmax) AS add_to_cart_vertical_pixels_scrolled,
        MAX(d.br_viewheight) AS add_to_cart_viewport_length,
        round(add_to_cart_vertical_pixels_scrolled / NULLIF(add_to_cart_viewport_length, 0), 2) + 1 AS add_to_cart_viewports_consumed,
        MAX(d.doc_height) AS add_to_cart_page_length

    from event_to_user_map as m
        inner join add_to_cart as e on m.event_id = e.event_id
        left join web_page_context as c on e.event_id = c.event_id
        left join web_events_time as t on c.page_view_id = t.page_view_id
        left join web_events_scroll_depth as d on c.page_view_id = d.page_view_id

    group by
        m.inferred_user_id
)

select * from add_to_cart_with_user_id
