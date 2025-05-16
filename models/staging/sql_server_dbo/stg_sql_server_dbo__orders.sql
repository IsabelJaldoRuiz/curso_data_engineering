{{
  config(
    materialized='view'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM  {{ ref('base_sql_server_dbo__orders') }}
),

renamed_casted AS (
    SELECT
        order_id 
        , {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_id 
        , shipping_cost
        , address_id
        , created_at 
        , promo_id
        , estimated_delivery_at
        , order_cost
        , user_id
        , order_total
        , delivered_at
        , tracking_id
        , status
        , is_deleted
        , date_load 
    FROM stg_orders
    )

SELECT * FROM renamed_casted