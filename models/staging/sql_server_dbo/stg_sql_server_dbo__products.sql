{{
  config(
    materialized='view'
  )
}}

WITH bases_products_products_info AS (
    SELECT products.*
        , weight_kg
        , height_cm
        , length_cm
        , width_cm
    FROM {{ ref('base_sql_server_dbo__products') }} AS products
    INNER JOIN
    {{ ref('base_additional_data__products_info') }} AS products_info
    ON products.name = products_info.name
    ),

stg_products AS (
    SELECT
        product_id
        , name
        , inventory
        , weight_kg
        , height_cm
        , length_cm
        , width_cm
        , is_deleted
        , date_load 
    FROM bases_products_products_info
    )

SELECT * FROM stg_products