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
        , CAST( IFNULL (_fivetran_deleted, FALSE) AS BOOLEAN ) AS is_deleted
        , CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) AS date_load
    FROM src_order_items
    )

SELECT * FROM renamed_casted