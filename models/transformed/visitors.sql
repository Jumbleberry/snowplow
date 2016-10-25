
{{
    config({
        "materialized" : "incremental",
        "distkey"      : "blended_user_id",
        "sortkey"      : "blended_user_id",
        "post-hook"    : "INSERT INTO {{ ref('queries') }} (SELECT 'visitors', 'new', {{ var('now') }} )",
        "unique_key"   : "blended_user_id",
        "sql_where"    : "last_touch_tstamp > (select max(first_touch_tstamp) from {{this}})"
    })
}}

-- Visitors:
-- (a) aggregate events into visitors
-- (b) select landing page
-- (c) select source
-- (d) combine in a single table

WITH enriched_events as (

    select * from {{ ref('snowplow_enriched_events') }}

),
basic AS (

  -- (a) aggregate events into visitors

  SELECT

    blended_user_id,

    MIN(collector_tstamp) AS first_touch_tstamp,
    MAX(collector_tstamp) AS last_touch_tstamp,
    MIN(dvce_created_tstamp) AS min_dvce_created_tstamp, -- used to replace SQL window functions
    MAX(dvce_created_tstamp) AS max_dvce_created_tstamp, -- used to replace SQL window functions
    MAX(etl_tstamp) AS max_etl_tstamp, -- for debugging

    COUNT(*) AS event_count,
    MAX(domain_sessionidx) AS session_count,
    SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
    COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM dvce_created_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes

  FROM enriched_events

  GROUP BY 1
  ORDER BY 1

), landing_page AS (

  -- (b) select landing page

  SELECT * FROM (
    SELECT -- select the first value for each column

      a.blended_user_id,

      a.page_urlhost,
      a.page_urlpath,

      ROW_NUMBER() OVER (PARTITION BY a.blended_user_id) AS row_number

    FROM enriched_events AS a

    INNER JOIN basic AS b
      ON  a.blended_user_id = b.blended_user_id
      AND a.dvce_created_tstamp = b.min_dvce_created_tstamp -- replaces the FIRST VALUE window function in SQL

    ORDER BY 1
  ) sbq
  WHERE row_number = 1 -- deduplicate

), source AS (

  -- (c) select source

  SELECT * FROM (
    SELECT

      a.blended_user_id,

      a.mkt_source,
      a.mkt_medium,
      a.mkt_term,
      a.mkt_content,
      a.mkt_campaign,

      a.refr_source,
      a.refr_medium,
      a.refr_term,
      a.refr_urlhost,
      a.refr_urlpath,

      ROW_NUMBER() OVER (PARTITION BY a.blended_user_id) AS row_number

    FROM enriched_events AS a

    INNER JOIN basic AS b
      ON  a.blended_user_id = b.blended_user_id
      AND a.dvce_created_tstamp = b.min_dvce_created_tstamp

    ORDER BY 1
  ) sbq
  WHERE row_number = 1 -- deduplicate

)

-- (d) combine in a single table

SELECT

  b.blended_user_id,

  b.first_touch_tstamp,
  b.last_touch_tstamp,
  b.min_dvce_created_tstamp,
  b.max_dvce_created_tstamp,
  b.max_etl_tstamp,
  b.event_count,
  b.session_count,
  b.page_view_count,
  b.time_engaged_with_minutes,

  l.page_urlhost AS landing_page_host,
  l.page_urlpath AS landing_page_path,

  s.mkt_source,
  s.mkt_medium,
  s.mkt_term,
  s.mkt_content,
  s.mkt_campaign,
  s.refr_source,
  s.refr_medium,
  s.refr_term,
  s.refr_urlhost,
  s.refr_urlpath

FROM basic AS b

LEFT JOIN landing_page AS l ON b.blended_user_id = l.blended_user_id
LEFT JOIN source AS s ON b.blended_user_id = s.blended_user_id
