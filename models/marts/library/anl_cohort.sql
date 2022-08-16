--enable this model in the models section of dbt_project.yml
{{ config(
    materialized = 'table'
)}}
{% set cohort_period = 'month'    %}               -- "week"|"month" weeks begin on Sundays
{% set cohort_table = 'fct_ga4__sessions' %}       -- the table you want to join on user_key against dim_ga4__users to create a cohort
{% set cohort_column_date = 'session_start_date'%} -- the datetime column from the cohort_table that you want to create a cohort on
{% set first_cohort_date = '2022-01-01' %}         -- cohort start including this day
{% set num_cohorts = 6 %}                          -- the number of cohorts that you want
-- variables override using dbt_project.yml file 
-- var:
--   ga4:
--     models:
--       marts:
--         library:
--           anl_cohort:
--             first_cohort_date: '2021-01-01'

-- or override using the vars flag with dbt run
-- dbt run --select anl_cohort  --vars '{"cohort_period":"week", "cohort_table":"fct_ga4__purchases", "cohort_column_date":"event_date_dt", "first_cohort_date":"2021-01-01","num_cohorts":12}'

with initial as (
    select
        user_key,
        event_date_dt,
        concat( extract( year from  first_seen_dt ), "-" ,extract( month from  first_seen_dt ) 
            {% if var('cohort_period') == 'week'  %}
                , "-" , extract( week from  first_seen_dt ) 
            {% endif %} 
        ) as initial_cohort
    from  {{ ref('dim_ga4__users') }}
    where event_date_dt >= date( {{'first_cohort_date' }} )
),
cohorts as (
    select
        user_key,
        {% for num in range( num_cohorts ) %}
            sum(case when date_diff(initial.initial_cohort, {{ cohort_column_date  }}, {{ cohort_period }}  ) = {{ num }} then 1 end) as {{cohort_period}}_{{num}},
        {% endfor %}
        initial.initial_cohort
    from  {{ ref( cohort_table ) }}
    right join initial using(user_key)
)
select
    *
from cohorts