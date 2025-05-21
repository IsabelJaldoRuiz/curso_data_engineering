{{
    config(
        materialized = "incremental",
        unique_key = "date_day"
    )
}}

{% set start_date = "2021-01-01" %}
{% set end_date = get_future_date(1) %}

WITH dim_date AS (
    {{ dbt_date.get_date_dimension(start_date, end_date) }}
),

renamed_casted AS (
    SELECT
        DATE_DAY AS date_day
        , PRIOR_DATE_DAY AS prior_date_day
        , NEXT_DATE_DAY AS next_date_day
        , PRIOR_YEAR_DATE_DAY AS prior_year_date_day
        , PRIOR_YEAR_OVER_YEAR_DATE_DAY AS prior_year_over_year_date_day
        , DAY_OF_WEEK_ISO AS day_of_week
        , DAY_OF_WEEK_NAME AS day_of_week_name
        , DAY_OF_WEEK_NAME_SHORT AS day_of_week_name_short
        , DAY_OF_MONTH AS day_of_month
        , DAY_OF_YEAR AS day_of_year
        , ISO_WEEK_START_DATE AS week_start_date
        , ISO_WEEK_END_DATE AS week_end_date
        , PRIOR_YEAR_ISO_WEEK_START_DATE AS prior_year_week_start_date
        , PRIOR_YEAR_ISO_WEEK_END_DATE AS prior_year_week_end_date
        , ISO_WEEK_OF_YEAR AS week_of_year
        , PRIOR_YEAR_ISO_WEEK_OF_YEAR AS prior_year_week_of_year
        , MONTH_OF_YEAR AS month_of_year
        , MONTH_NAME AS month_name
        , MONTH_NAME_SHORT AS month_name_short
        , MONTH_START_DATE AS month_start_date
        , MONTH_END_DATE AS month_end_date
        , PRIOR_YEAR_MONTH_START_DATE AS prior_year_month_start_date
        , PRIOR_YEAR_MONTH_END_DATE AS prior_year_month_end_date
        , QUARTER_OF_YEAR AS quarter_of_year
        , QUARTER_START_DATE AS quarter_start_date
        , QUARTER_END_DATE AS quarter_end_date
        , YEAR_NUMBER AS year_number
        , YEAR_START_DATE AS year_start_date
        , YEAR_END_DATE AS year_end_date
    FROM dim_date
    )

SELECT * FROM renamed_casted