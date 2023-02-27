
{{
    config(
        materialized='table',
        sort='page_view_start',
        dist='user_snowplow_domain_id',
        unique_key='user_snowplow_domain_id'
    )
}}


with engagements as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'Engagement'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

leads as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'Lead'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

viewcontent as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'ViewContent'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

completeregistration as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'CompleteRegistration'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

initiatecheckout as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'InitiateCheckout'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

addpaymentinfo as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'AddPaymentInfo'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

purchase as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'Purchase'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

upsell as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'Upsell'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

ad_view as (
select 
user_snowplow_domain_id
from dataform.snowplow_page_views 
where page_title = 'AddView'
{% if target.name == 'dev' %}
    and page_view_start between dateadd('month', -2, current_date) and dateadd('month', -1, current_date)
{% endif %}
group by 1
),

prep as (
select
user_snowplow_domain_id,
page_view_start,
page_url as first_page_url,
page_url_path as first_page_url_path,
page_title as event,
marketing_medium,
marketing_source,
marketing_term,
marketing_content,
marketing_campaign
from dataform.snowplow_page_views 
where page_view_index = '1'
),

final as (select
p.*,
count(eg.user_snowplow_domain_id) as engagements,
count(ld.user_snowplow_domain_id) as leads,
count(vc.user_snowplow_domain_id) as viewcontent,
count(cr.user_snowplow_domain_id) as completeregistration,
count(ic.user_snowplow_domain_id) as initiatecheckout,
count(ap.user_snowplow_domain_id) as addpaymentinfo,
count(pr.user_snowplow_domain_id) as purchase,
count(up.user_snowplow_domain_id) as upsell,
count(av.user_snowplow_domain_id) as ad_view

from prep p
left join engagements eg
on p.user_snowplow_domain_id = eg.user_snowplow_domain_id
left join leads ld
on p.user_snowplow_domain_id = ld.user_snowplow_domain_id
left join viewcontent vc
on p.user_snowplow_domain_id = vc.user_snowplow_domain_id
left join completeregistration cr
on p.user_snowplow_domain_id = cr.user_snowplow_domain_id
left join initiatecheckout ic
on p.user_snowplow_domain_id = ic.user_snowplow_domain_id
left join addpaymentinfo ap
on p.user_snowplow_domain_id = ap.user_snowplow_domain_id
left join purchase pr
on p.user_snowplow_domain_id = pr.user_snowplow_domain_id
left join upsell up
on p.user_snowplow_domain_id = pr.user_snowplow_domain_id
left join ad_view av
on p.user_snowplow_domain_id = av.user_snowplow_domain_id

group by 1,2,3,4,5,6,7,8,9,10
)

select * from final
