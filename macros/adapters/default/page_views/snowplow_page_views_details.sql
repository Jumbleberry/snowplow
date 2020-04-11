
{% macro snowplow_page_views_details() %}

    {{ adapter_macro('snowplow.snowplow_page_views_details') }}

{% endmacro %}


{% macro default__snowplow_page_views_details() %}
-- depends_on: {{ ref('snowplow_web_events_details') }}

{{
    config(
        materialized='incremental',
        sort='max_tstamp',
        dist='user_snowplow_domain_id',
        unique_key='event_id'
    )
}}


-- initializations
{% set timezone = var('snowplow:timezone', 'UTC') %}

{% set use_perf_timing = (var('snowplow:context:performance_timing') != false) %}
{% set use_useragents = (var('snowplow:context:useragent') != false) %}

-- we are using 1-days window allowing us cover most of sessions
-- but model become much faster comparing to selecting all events
with all_events as (

    select * from {{ ref('snowplow_web_events_details') }}
    {% if is_incremental() %}
    where collector_tstamp > (
        DATEADD('day', -1 * {{var('snowplow:page_view_lookback_days')}}, (select coalesce(max(max_tstamp), '0001-01-01') from {{ this }}))
        )
    {% endif %}
),

filtered_events as (

    select * from all_events
    {% if is_incremental() %}
    where collector_tstamp > (
        select coalesce(max(max_tstamp), '0001-01-01') from {{ this }}
    )
    {% endif %}

),

-- we need to grab all events for any session that has appeared
-- in order to correctly process the session index below
relevant_sessions as (

    select distinct domain_sessionid
    from filtered_events
),

web_events as (

    select all_events.*
    from all_events
    join relevant_sessions using (domain_sessionid)

),

web_events_time as (

    select * from {{ ref('snowplow_web_events_time_details') }}

),

web_events_scroll_depth as (

    select * from {{ ref('snowplow_web_events_scroll_depth_details') }}

),

{% if use_perf_timing != false %}

    web_timing_context as ( select * from {{ ref('snowplow_web_timing_context') }} ),

{% endif %}

{% if use_useragents != false %}

    web_ua_parser_context as ( select * from {{ ref('snowplow_web_ua_parser_context') }} ),

{% endif %}

prep as (

    select
        -- user
        a.user_id as user_custom_id,
        a.domain_userid as user_snowplow_domain_id,
        a.network_userid as user_snowplow_crossdomain_id,
        REGEXP_SUBSTR(a.page_urlquery, '[0-9][0-9][0-9][0-9][0-9][0-9]') as affiliate_id,

        b.min_tstamp,
        b.max_tstamp,

        CONVERT_TIMEZONE('UTC', '{{ timezone }}', b.min_tstamp) as page_view_start,
        CONVERT_TIMEZONE('UTC', '{{ timezone }}', b.max_tstamp) as page_view_end,
        convert_timezone('UTC', coalesce(a.os_timezone, '{{ timezone }}'), b.min_tstamp) as page_view_start_local,
        convert_timezone('UTC', coalesce(a.os_timezone, '{{ timezone }}'), b.max_tstamp) as page_view_end_local,

        -- sesssion
        a.domain_sessionid as session_id,
        a.domain_sessionidx as session_index,

        -- page view
        a.page_view_id,

        -- event
        a.event_id,
        -- engagement
        b.time_engaged_in_s,

        case
            when b.time_engaged_in_s >= 180 then '180s_stay'
            when b.time_engaged_in_s >= 60 then '60s_stay'
            when b.time_engaged_in_s >= 30 then '30s_stay'
            when b.time_engaged_in_s >= 11 then '11s_stay'
            when b.time_engaged_in_s >= 5 then '5s_stay'
            else null
        end as time_engaged_in_s_tier,

        c.hmax as horizontal_pixels_scrolled,
        c.vmax as vertical_pixels_scrolled,

        c.relative_hmax as horizontal_percentage_scrolled,
        c.relative_vmax as vertical_percentage_scrolled,

        case
            when c.relative_vmax between 0 and 19 then 'viewport_1'
            when c.relative_vmax between 20 and 39 then 'viewport_2'
            when c.relative_vmax between 40 and 59 then 'viewport_3'
            when c.relative_vmax between 60 and 79 then 'viewport_4'
            when c.relative_vmax between 80 and 100 then 'viewport_5'
            else null
        end as vertical_percentage_scrolled_tier,

        -- page
        a.page_urlhost || a.page_urlpath as page_url,

        a.page_urlscheme as page_url_scheme,
        a.page_urlhost as page_url_host,
        a.page_urlport as page_url_port,
        a.page_urlpath as page_url_path,
        a.page_urlquery as page_url_query,
        a.page_urlfragment as page_url_fragment,

        a.page_title,

        c.doc_width as page_width,
        c.doc_height as page_height,

        -- referer
        a.refr_urlhost || a.refr_urlpath as referer_url,

        a.refr_urlscheme as referer_url_scheme,
        a.refr_urlhost as referer_url_host,
        a.refr_urlport as referer_url_port,
        a.refr_urlpath as referer_url_path,
        a.refr_urlquery as referer_url_query,
        a.refr_urlfragment as referer_url_fragment,

        case
        when a.refr_medium is null then 'direct'
        when a.refr_medium = 'unknown' then 'other'
        else a.refr_medium
        end as referer_medium,
        a.refr_source as referer_source,
        a.refr_term as referer_term,

        -- marketing
        a.mkt_medium as marketing_medium,
        a.mkt_source as marketing_source,
        a.mkt_term as marketing_term,
        a.mkt_content as marketing_content,
        a.mkt_campaign as marketing_campaign,
        -- these come straight from the event
        a.mkt_clickid as marketing_click_id,
        a.mkt_network as marketing_network,

        -- location
        a.geo_country,
        a.geo_region,
        a.geo_region_name,
        a.geo_city,
        a.geo_zipcode,
        a.geo_latitude,
        a.geo_longitude,
        a.geo_timezone, -- often null (use os_timezone instead)

        -- ip
        a.user_ipaddress as ip_address,
        a.ip_isp,
        a.ip_organization,
        a.ip_domain,
        a.ip_netspeed as ip_net_speed,

        -- application
        a.app_id,

        {% if use_useragents %}
            d.useragent_version as browser,
            d.useragent_family as browser_name,
            d.useragent_major as browser_major_version,
            d.useragent_minor as browser_minor_version,
            d.useragent_patch as browser_build_version,
            d.os_version as os,
            d.os_family as os_name,
            d.os_major as os_major_version,
            d.os_minor as os_minor_version,
            d.os_patch as os_build_version,
            d.device_family as device,
        {% else %}
            null::text as browser,
            a.br_family as browser_name,
            a.br_name as browser_major_version,
            a.br_version as browser_minor_version,
            null::text as browser_build_version,
            a.os_family as os,
            a.os_name as os_name,
            null::text as os_major_version,
            null::text as os_minor_version,
            null::text as os_build_version,
            null::text as device,
        {% endif %}

        c.br_viewwidth as browser_window_width,
        c.br_viewheight as browser_window_height,

        a.br_lang as browser_language,

        -- os
        a.os_manufacturer,
        a.os_timezone,

        {% if use_perf_timing %}
            e.redirect_time_in_ms,
            e.unload_time_in_ms,
            e.app_cache_time_in_ms,
            e.dns_time_in_ms,
            e.tcp_time_in_ms,
            e.request_time_in_ms,
            e.response_time_in_ms,
            e.processing_time_in_ms,
            e.dom_loading_to_interactive_time_in_ms,
            e.dom_interactive_to_complete_time_in_ms,
            e.onload_time_in_ms,
            e.total_time_in_ms,
        {% else %}
            null::bigint as redirect_time_in_ms,
            null::bigint as unload_time_in_ms,
            null::bigint as app_cache_time_in_ms,
            null::bigint as dns_time_in_ms,
            null::bigint as tcp_time_in_ms,
            null::bigint as request_time_in_ms,
            null::bigint as response_time_in_ms,
            null::bigint as processing_time_in_ms,
            null::bigint as dom_loading_to_interactive_time_in_ms,
            null::bigint as dom_interactive_to_complete_time_in_ms,
            null::bigint as onload_time_in_ms,
            null::bigint as total_time_in_ms,
        {% endif %}

        -- device
        a.br_renderengine as browser_engine,
        a.dvce_type as device_type,
        a.dvce_ismobile as device_is_mobile
        
        {%- for column in var('snowplow:pass_through_columns') %}
        , a.{{column}}
        {% endfor %}

    from web_events as a
        inner join web_events_time as b 
        on a.page_view_id = b.page_view_id
        and a.event_id = b.event_id
        
        inner join web_events_scroll_depth as c 
        on a.page_view_id = c.page_view_id
        and a.event_id = c.event_id

        {% if use_useragents %}

            left outer join web_ua_parser_context as d on a.page_view_id = d.page_view_id

        {% endif %}

        {% if use_perf_timing %}

            left outer join web_timing_context as e on a.page_view_id = e.page_view_id

        {% endif %}

    where (a.br_family != 'Robot/Spider' or a.br_family is null)
      and not (
    (useragent like '%bot%'
    or useragent like '%crawl%'
    or useragent like '%slurp%'
    or useragent like '%spider%'
    or useragent like '%archiv%'
    or useragent like '%spinn%'
    or useragent like '%sniff%'
    or useragent like '%seo%'
    or useragent like '%audit%'
    or useragent like '%survey%'
    or useragent like '%pingdom%'
    or useragent like '%worm%'
    or useragent like '%capture%'
    or useragent like '%browsershots%'
    or useragent like '%screenshots%'
    or useragent like '%analyz%'
    or useragent like '%index%'
    or useragent like '%thumb%'
    or useragent like '%check%'
    or useragent like '%facebook%'
    or useragent like '%PingdomBot%'
    or useragent like '%PhantomJS%'
    or useragent like '%YorexBot%'
    or useragent like '%Twitterbot%'
    or useragent like '%a_archiver%'
    or useragent like '%facebookexternalhit%'
    or useragent like '%Bingbot%'
    or useragent like '%BingPreview%'
    or useragent like '%Googlebot%'
    or useragent like '%Baiduspider%'
    or useragent like '%360Spider%'
    or useragent like '%360User-agent%'
    or useragent like '%semalt%')
        or a.useragent is null)
      and coalesce(a.br_type, 'unknown') not in ('Bot/Crawler', 'Robot')
      and a.domain_userid is not null
      and a.domain_sessionidx > 0

)

select * from prep

{% endmacro %}