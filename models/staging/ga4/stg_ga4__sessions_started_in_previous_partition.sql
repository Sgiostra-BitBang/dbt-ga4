-- considered partitioning this table to make _dbt_max_partition available; not worth the effort for a POC
{% set previous_partition = ('date_sub(current_date, interval ' + (var('static_incremental_days', 1 )+1)|string + ' day)')  %}
{{
    config(
        materialized = 'view'
    )
}}

with add_user_key as (
    select 
        case
            when user_id is not null then to_base64(md5(user_id))
            when user_pseudo_id is not null then to_base64(md5(user_pseudo_id))
            else null -- this case is reached when privacy settings are enabled
        end as user_key,
        stream_id,
        ga_session_id,
        event_timestamp
    from {{ ref('base_ga4__events') }}
    where event_date_dt = {{ previous_partition }}
    and timestamp_micros(event_timestamp) > timestamp_sub( timestamp_micros((  
        select
            max(event_timestamp)
        from {{ ref('base_ga4__events') }}
        where event_date_dt = {{ previous_partition }}    
    )), interval {{ var('session_duration_minutes', 30 ) }} minute)
    ), 
include_session_key as (
    select 
        *,
        to_base64(md5(CONCAT(stream_id, CAST(user_key as STRING), cast(ga_session_id as STRING)))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from add_user_key
)
select distinct
    session_key 
from include_session_key
