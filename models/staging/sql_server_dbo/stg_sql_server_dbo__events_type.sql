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
        {{ dbt_utils.generate_surrogate_key(['event_type']) }} AS event_type_id
        , event_type
    FROM src_events
    GROUP BY event_type
    )

SELECT * FROM renamed_casted