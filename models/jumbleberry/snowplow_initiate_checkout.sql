{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

{% set timezone = var('snowplow:timezone', 'UTC') %}

with initiate_checkout as (

    select * from {{ var('snowplow:initiate_checkout') }}

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

initiate_checkout_with_user_id as (

    select 
        m.inferred_user_id,
        count(e.*) as initiate_checkout_count,
        SUM(t.pv_count) AS initiate_checkout_pv_count,
        SUM(t.pp_count) AS initiate_checkout_pp_count,
        SUM(t.time_engaged_in_s) AS initiate_checkout_time_engaged_in_s,
        {{convert_timezone("'UTC'", "'" ~ timezone ~ "'", 'MIN(t.min_tstamp)')}} as initiate_checkout_first_time,
        MAX(d.vmax) AS initiate_checkout_vertical_pixels_scrolled,
        MAX(d.br_viewheight) AS initiate_checkout_viewport_length,
        round(initiate_checkout_vertical_pixels_scrolled / NULLIF(initiate_checkout_viewport_length, 0), 2) + 1 AS initiate_checkout_viewports_consumed

    from event_to_user_map as m
        inner join initiate_checkout as e on m.event_id = e.event_id
        left join web_page_context as c on e.event_id = c.event_id
        left join web_events_time as t on c.page_view_id = t.page_view_id
        left join web_events_scroll_depth as d on c.page_view_id = d.page_view_id

    group by
        m.inferred_user_id
)

select * from initiate_checkout_with_user_id
