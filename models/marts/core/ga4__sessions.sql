-- Fact table for sessions. Join on session_key

with first_pv_key as (
    select
        session_key,
        first_page_view_event_key
    from  {{ref('stg_ga4__sessions_first_last_pageviews')}}
), 
first_page_view as (
    select 
        first_pv_key.session_key as session_key,
        event_date_dt as session_start_date,
        event_timestamp as session_start_timestamp,
        page_location as landing_page,
        page_hostname as landing_page_hostname,
        geo_continent,
        geo_country,
        geo_region,
        geo_city,
        geo_sub_continent,
        geo_metro,
        device_category,
        device_mobile_brand_name,
        device_mobile_model_name,
        device_mobile_marketing_name,
        device_mobile_os_hardware_model,
        device_operating_system,
        device_operating_system_version,
        device_vendor_id,
        device_advertising_id,
        device_language,
        device_is_limited_ad_tracking,
        device_time_zone_offset_seconds,
        device_browser,
        device_web_info_browser,
        device_web_info_browser_version,
        device_web_info_hostname,
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
    from {{ref('stg_ga4__events')}} as events
    right join first_pv_key on first_pv_key.first_page_view_event_key = events.event_key
),
session_metrics as 
(
    select 
        session_key,
        user_key,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec,
    from {{ref('stg_ga4__events')}}
    left join first_page_view using(session_key)
    group by 1,2
),
include_session_properties as (
    select * from session_metrics
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} using (session_key)
    {% endif %}
)
{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from include_session_properties
    left join {{ref('stg_ga4__session_conversions')}} using (session_key)
),
select * from join_conversions
{% else %}
select * from include_session_properties
{% endif %}

