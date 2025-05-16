WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} AS VARCHAR ) AS order_item_id  
        , CAST( {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS VARCHAR ) AS order_id
        , CAST( {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS VARCHAR ) AS product_id
        , CAST( quantity AS NUMBER ) AS quantity
        , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_order_items
    )

SELECT * FROM renamed_casted