{{ config(
    materialized= 'incremental',
    unique_key='page_key'
)
}}
-- get the last value for ephemeral dimensions each day
-- todo: add custom dimensions
with page_view as (
    select
        page_key,
        event_date_dt,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        last_page_title_page_key
        {% if var("dim_ga4__pages_custom_parameters", "none") != "none" %}  
            {{ ga4.mart_custom_parameters( var("dim_ga4__pages_custom_parameters") )}} 
        {% endif %}
    from (
        select
            page_key,
            event_date_dt,
            page_location,
            last_value(page_title) OVER (PARTITION BY page_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_title_page_key
            {% if var("dim_ga4__pages_custom_parameters", "none") != "none" %}  
                {{ ga4.mart_custom_parameters( var("dim_ga4__pages_custom_parameters") )}} 
            {% endif %}
        from {{ref('stg_ga4__event_page_view')}}
    )
    group by 1,2,3,4
    {% if var("dim_ga4__pages_custom_parameters", "none") != "none" %}  
        {{ ga4.mart_group_by_custom_parameters( var("dim_ga4__pages_custom_parameters") )}} 
    {% endif %}
)
select
    *
from page_view