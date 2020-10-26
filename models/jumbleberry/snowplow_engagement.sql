{{
    config(
        sort='inferred_user_id',
        dist='inferred_user_id',
        unique_key='inferred_user_id'
    )
}}

{% set timezone = var('snowplow:timezone', 'UTC') %}

with engagement as (

    select * from {{ var('snowplow:engagement') }}

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

engagement_with_user_id as (

    select 
        m.inferred_user_id,
        max(e.c_1) as c_1,
        max(e.c_2) as c_2,
        max(e.c_3) as c_3,
        count(e.*) as engagement_count,
        SUM(t.pv_count) AS engagement_pv_count,
        SUM(t.pp_count) AS engagement_pp_count,
        SUM(t.time_engaged_in_s) AS engagement_time_engaged_in_s,
        {{convert_timezone("'UTC'", "'" ~ timezone ~ "'", 'MIN(t.min_tstamp)')}} as engagement_first_time,
        MAX(d.vmax) AS engagement_vertical_pixels_scrolled,
        MAX(d.br_viewheight) AS engagement_viewport_length,
        round(engagement_vertical_pixels_scrolled / NULLIF(engagement_viewport_length, 0), 2) + 1 AS engagement_viewports_consumed

    from event_to_user_map as m
        inner join engagement as e on m.event_id = e.event_id
        left join web_page_context as c on e.event_id = c.event_id
        left join web_events_time as t on c.page_view_id = t.page_view_id
        left join web_events_scroll_depth as d on c.page_view_id = d.page_view_id

    group by
        m.inferred_user_id
)

select * from engagement_with_user_id
