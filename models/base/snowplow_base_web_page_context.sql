
select * from {{ var('snowplow:context:web_page') }}

{% if target.name == 'dev' %}
  where _fivetran_synced between dbt_utils.dateadd('month', -2, current_date) and dbt_utils.dateadd('month', -1, current_date)
{% endif %}