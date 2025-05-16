{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    ),

renamed_casted AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_id
        , shipping_service
    FROM src_orders
    GROUP BY shipping_service
    
    UNION ALL 
    
    SELECT 
        {{ dbt_utils.generate_surrogate_key(["'non-service'"]) }}
        , 'non-service'
    )


SELECT * FROM renamed_casted