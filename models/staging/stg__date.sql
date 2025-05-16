{{
    config(
        materialized = "view"
    )
}}

{% set start_date = "2015-01-01" %}
{% set end_date = get_future_date(2) %}

WITH stg_date AS (
    {{ dbt_date.get_date_dimension(start_date, end_date) }}
),

renamed_casted AS (
    SELECT
        DATE_DAY
        , PRIOR_DATE_DAY
        , NEXT_DATE_DAY
        , PRIOR_YEAR_DATE_DAY
        , PRIOR_YEAR_OVER_YEAR_DATE_DAY
        , DAY_OF_WEEK_ISO AS DAY_OF_WEEK
        , DAY_OF_WEEK_NAME
        , DAY_OF_WEEK_NAME_SHORT
        , DAY_OF_MONTH
        , DAY_OF_YEAR
        , ISO_WEEK_START_DATE AS WEEK_START_DATE
        , ISO_WEEK_END_DATE AS WEEK_END_DATE
        , PRIOR_YEAR_ISO_WEEK_START_DATE AS PRIOR_YEAR_WEEK_START_DATE
        , PRIOR_YEAR_ISO_WEEK_END_DATE AS PRIOR_YEAR_WEEK_END_DATE
        , ISO_WEEK_OF_YEAR AS WEEK_OF_YEAR
        , PRIOR_YEAR_ISO_WEEK_OF_YEAR AS PRIOR_YEAR_WEEK_OF_YEAR
        , MONTH_OF_YEAR
        , MONTH_NAME
        , MONTH_NAME_SHORT
        , MONTH_START_DATE
        , MONTH_END_DATE
        , PRIOR_YEAR_MONTH_START_DATE
        , PRIOR_YEAR_MONTH_END_DATE
        , QUARTER_OF_YEAR
        , QUARTER_START_DATE
        , QUARTER_END_DATE
        , YEAR_NUMBER
        , YEAR_START_DATE
        , YEAR_END_DATE

    FROM stg_date
    )

SELECT * FROM renamed_casted