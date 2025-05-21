WITH stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__order_items') }} AS order_items
    ),

dim_order_items AS (
    SELECT
        order_item_id  
        , order_id
        , product_id
        , quantity
        , price
        , is_deleted
        , date_load 
    FROM stg_order_items
    )

SELECT * FROM dim_order_items