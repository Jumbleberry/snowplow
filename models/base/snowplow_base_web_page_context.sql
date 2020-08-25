
select * from {{ var('snowplow:context:web_page') }}

{% if target.name == 'dev' %}
  where _fivetran_synced >= dateadd('day', -1, current_date)
{% endif %}