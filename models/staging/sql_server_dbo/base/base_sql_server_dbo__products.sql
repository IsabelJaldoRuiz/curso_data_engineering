WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
        {{ dbt.hash('product_id') }} AS product_id 
        , price
        , name
        , inventory
        , _fivetran_deleted AS is_deleted
        , CONVERT_TIMEZONE('UTC', _fivetran_synced::TIMESTAMP_TZ(9)) AS date_load
    FROM src_products
    )

SELECT * FROM renamed_casted