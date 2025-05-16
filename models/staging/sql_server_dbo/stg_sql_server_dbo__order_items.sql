WITH src_order_items AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__order_items') }}
    ),

renamed_casted AS (
    SELECT
        order_item_id  
        , order_id
        , product_id
        , quantity
        , is_deleted
        , date_load 
    FROM src_order_items
    )

SELECT * FROM renamed_casted