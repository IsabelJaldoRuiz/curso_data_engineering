WITH stg_products AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__products') }} 
    ),

dim_products AS (
    SELECT
        product_id
        , name
        , inventory
        , weight_kg
        , height_cm
        , length_cm
        , width_cm
    FROM stg_products
    )

SELECT * FROM dim_products