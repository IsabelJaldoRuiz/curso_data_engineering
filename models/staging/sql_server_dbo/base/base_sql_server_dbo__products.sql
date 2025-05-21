WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS VARCHAR ) AS product_id
        ---, product_id AS old_product_id 
        , CAST( price AS FLOAT ) AS price
        , CAST( name AS VARCHAR ) AS name
        , CAST( inventory AS NUMBER ) AS inventory
        , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_products
    )

SELECT * FROM renamed_casted