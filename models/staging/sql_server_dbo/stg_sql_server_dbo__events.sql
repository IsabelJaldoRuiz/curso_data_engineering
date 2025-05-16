{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__events') }}
    ),

renamed_casted AS (
    SELECT
        event_id
        , page_url
        , {{ dbt_utils.generate_surrogate_key(['event_type']) }} AS event_type_id
        , user_id
        , product_id
        , session_id
        , created_at
        , order_id
        , is_deleted
        , date_load 
    FROM src_events
    )

SELECT * FROM renamed_casted