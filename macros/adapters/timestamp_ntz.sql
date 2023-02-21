{% macro snowplow_timestamp_ntz(field) %}
    {{ return(adapter.dispatch('snowplow_timestamp_ntz')(field)) }}
{% endmacro %}

{% macro default__snowplow_timestamp_ntz(field) %}
    {{field}}
{%- endmacro %}

{% macro snowflake__snowplow_timestamp_ntz(field) %}
    {{field}}::timestamp_ntz
{% endmacro %}
