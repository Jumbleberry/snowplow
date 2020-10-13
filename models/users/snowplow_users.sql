
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

declines as (

    select * from {{ ref('snowplow_declines') }}

),

chargebacks as (

    select * from {{ ref('snowplow_chargebacks') }}

),

purchases as (

    select * from {{ ref('snowplow_purchases') }}

),

upsells as (

    select * from {{ ref('snowplow_upsells') }}

),

web_events_scroll_depth as (

    select * from {{ ref('snowplow_web_events_scroll_depth') }}

),

pv as (

    select * from {{ ref('snowplow_page_views') }}

),

wesd as (

    select * from {{ ref('snowplow_web_events_scroll_depth') }}

),

wet as (
  select * from {{ ref('snowplow_web_events_time') }}
),

scroll_depth as (
  SELECT
  pv.user_snowplow_domain_id,
  MAX(wesd.br_viewheight) AS viewport_length

  {%- for column, eventName in var('jumbleberry:events').items() %}
    , MAX(CASE WHEN pv.page_title = '{{eventName}}' THEN wesd.vmax ELSE 0 END) AS {{column}}_vertical_pixels_scrolled
    , MAX(CASE WHEN pv.page_title = '{{eventName}}' THEN wesd.br_viewheight ELSE 0 END) AS {{column}}_viewport_length
    , round({{column}}_vertical_pixels_scrolled / NULLIF({{column}}_viewport_length, 0), 2) + 1 AS {{column}}_viewports_consumed
  {% endfor %}

  , (
    {%- for column, eventName  in var('jumbleberry:events').items() %}
      COALESCE({{column}}_vertical_pixels_scrolled, 0) +
    {% endfor %}
    0
  ) AS vertical_pixels_scrolled

  , (
    {%- for column, eventName  in var('jumbleberry:events').items() %}
      COALESCE({{column}}_viewports_consumed, 0) +
    {% endfor %}
    0
  ) AS viewports_consumed

  FROM pv
  LEFT JOIN wesd ON wesd.page_view_id = pv.page_view_id
  GROUP BY pv.user_snowplow_domain_id
),

time_ellapsed as (
  SELECT
  pv.user_snowplow_domain_id

  {%- for column, eventName  in var('jumbleberry:events').items() %}
    , SUM(CASE WHEN pv.page_title = '{{eventName}}' THEN wet.pv_count ELSE 0 END) AS {{column}}_pv_count
    , SUM(CASE WHEN pv.page_title = '{{eventName}}' THEN wet.pp_count ELSE 0 END) AS {{column}}_pp_count
    , SUM(CASE WHEN pv.page_title = '{{eventName}}' THEN wet.time_engaged_in_s ELSE 0 END) AS {{column}}_time_engaged_in_s
    , MIN(CASE WHEN pv.page_title = '{{eventName}}' THEN pv.page_view_start ELSE NULL END) AS {{column}}_page_view_start
  {% endfor %}

  , (
    {%- for column, eventName  in var('jumbleberry:events').items() %}
      COALESCE({{column}}_pp_count, 0) +
    {% endfor %}
    0
  ) AS page_pings
  

  FROM pv pv
  LEFT JOIN wet ON wet.page_view_id = pv.page_view_id
  GROUP BY pv.user_snowplow_domain_id
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

        -- derived dimensions
        to_char(b.first_session_start_local, 'YYYY-MM-DD HH24:MI:SS') as first_session_local_time,
        to_char(b.first_session_start_local, 'HH24:MI') as first_session_local_time_of_day,
        date_part('hour', b.first_session_start_local)::integer as first_session_local_hour_of_day,
        trim(to_char(b.first_session_start_local, 'd')) as first_session_local_day_of_week,
        mod(extract(dow from b.first_session_start_local)::integer - 1 + 7, 7) as first_session_local_day_of_week_index,

        -- last session: time
        b.last_session_end,
        to_char(b.last_session_end, 'YYYY-MM-DD HH24:MI:SS') as last_session_time,
        EXTRACT(EPOCH FROM (b.last_session_end - b.first_session_start)) as time_elapsed_in_s,

        -- engagement
        b.page_views,
        te.page_pings,
        b.sessions,
        b.time_engaged_in_s,
        sd.viewports_consumed,
        sd.viewport_length,
        sd.vertical_pixels_scrolled,

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

        -- declines
        d.decline_count,
        d.decline_total_value,

        -- chargebacks
        cb.chargeback_count,
        cb.chargeback_total_value,

        -- purchases
        p.purchase_count,
        p.purchase_max_value,
        p.purchase_total_value,

        -- upsells
        u.upsell_count,
        u.upsell_max_value,
        u.upsell_total_value

        -- event metrics
        {%- for column, eventName in var('jumbleberry:events').items() %}
          , sd.{{column}}_vertical_pixels_scrolled
          , sd.{{column}}_viewport_length
          , sd.{{column}}_viewports_consumed
          , te.{{column}}_pv_count
          , te.{{column}}_pp_count
          , te.{{column}}_time_engaged_in_s
          , to_char(te.{{column}}_page_view_start, 'YYYY-MM-DD HH24:MI:SS') as {{column}}_page_view_start
          , EXTRACT(EPOCH FROM (te.{{column}}_page_view_start - b.first_session_start)) as {{column}}_time_elapsed_in_s
        {% endfor %},

        GETDATE() AS updated

    from sessions as a
        inner join prep as b on a.inferred_user_id = b.inferred_user_id
        left join engagement as e on a.inferred_user_id = e.inferred_user_id
        left join declines as d on a.inferred_user_id = d.inferred_user_id
        left join chargebacks as cb on a.inferred_user_id = cb.inferred_user_id
        left join purchases as p on a.inferred_user_id = p.inferred_user_id
        left join upsells as u on a.inferred_user_id = u.inferred_user_id
        left join scroll_depth as sd on a.inferred_user_id = sd.user_snowplow_domain_id
        left join time_ellapsed as te on a.inferred_user_id = te.user_snowplow_domain_id

    where a.session_index = 1
)

select * from users where dedupe = 1
