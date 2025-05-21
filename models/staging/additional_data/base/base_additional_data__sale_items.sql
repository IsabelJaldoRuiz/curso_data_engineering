WITH src_sale_items AS (
    SELECT * 
    FROM {{ source('additional_data', 'sale_items') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['sale_id', 'product_id']) }} AS VARCHAR ) AS sale_item_id  
        , CAST( {{ dbt_utils.generate_surrogate_key(['sale_id']) }} AS VARCHAR ) AS sale_id
        , CAST( {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS VARCHAR ) AS product_id
        , CAST( quantity AS NUMBER ) AS quantity
        , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_sale_items
    )

SELECT * FROM renamed_casted