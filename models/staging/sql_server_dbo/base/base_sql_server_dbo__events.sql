WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS VARCHAR ) AS event_id
        -- , event_id AS old_event_id
        , CAST( page_url AS VARCHAR ) AS page_url
        , CAST( event_type AS VARCHAR ) AS event_type
        , CAST( {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS VARCHAR ) AS user_id
        , CAST( {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS VARCHAR ) AS product_id
        , CAST( {{ dbt_utils.generate_surrogate_key(['session_id']) }} AS VARCHAR ) AS session_id
        , CONVERT_TIMEZONE('UTC', CAST(created_at AS TIMESTAMP_NTZ)) AS created_at
        , CAST( {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS VARCHAR ) AS order_id
        , CAST( IFNULL (_fivetran_deleted, FALSE) AS BOOLEAN ) AS is_deleted
        , CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) AS date_load
    FROM src_events
    )

SELECT * FROM renamed_casted