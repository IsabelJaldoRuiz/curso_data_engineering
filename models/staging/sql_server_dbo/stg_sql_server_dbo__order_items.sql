{{
  config(
    materialized='view'
  )
}}

WITH bases_order_sale_items AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__order_items') }}
    
    UNION ALL
    
    SELECT * 
    FROM {{ ref('base_additional_data__sale_items') }}
    ), 

bases_order_items_products AS (
    SELECT order_items.*, products.price
    FROM bases_order_sale_items AS order_items
    INNER JOIN {{ ref('base_sql_server_dbo__products') }} AS products
    ON order_items.product_id = products.product_id
    ),

stg_order_items AS (
    SELECT
        order_item_id  
        , order_id
        , product_id
        , quantity
        , price
        , is_deleted
        , date_load 
    FROM bases_order_items_products
    )

SELECT * FROM stg_order_items