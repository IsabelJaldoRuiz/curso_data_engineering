WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
    ),

fct_events AS (
    SELECT
        event_id
        , event_type_id
        , product_id
        , user_id
        , session_id
        , order_id
        , page_url
        , DATE( created_at ) AS created_at_date
        , TIME( created_at ) AS created_at_time
    FROM stg_events
    )

SELECT * FROM fct_events
