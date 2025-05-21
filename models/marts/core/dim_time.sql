{{
    config(
        materialized = "incremental",
        unique_key = "time"
    )
}}

WITH seconds AS (
    SELECT 
        SEQ4() AS second_of_day
    FROM TABLE( GENERATOR( ROWCOUNT => 86400 ) )
),

time_dim AS (
    SELECT
        FLOOR( second_of_day / 3600 ) AS hour,
        FLOOR( MOD(second_of_day, 3600 ) / 60) AS minute,
        MOD( second_of_day, 60 ) AS second,
        TO_TIME(
            LPAD( FLOOR( second_of_day / 3600 )::STRING, 2, '0' ) || ':' ||
            LPAD( FLOOR( MOD(second_of_day, 3600 ) / 60)::STRING, 2, '0' ) || ':' ||
            LPAD( MOD( second_of_day, 60 )::STRING, 2, '0' )
        ) AS time
    FROM seconds
)

SELECT
    time,
    hour,
    minute,
    second
FROM time_dim

{% if is_incremental() %}

WHERE time NOT IN ( SELECT time FROM {{ this }} )

{% endif %}