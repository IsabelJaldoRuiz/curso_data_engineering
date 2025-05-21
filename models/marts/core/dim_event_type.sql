WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
    ),

dim_event_type AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['event_type']) }} AS event_type_id
        , event_type
    FROM stg_events
    GROUP BY event_type
    )

SELECT * FROM dim_event_type