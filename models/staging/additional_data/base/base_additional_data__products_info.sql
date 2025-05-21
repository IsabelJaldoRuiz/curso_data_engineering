WITH src_products_info AS (
    SELECT * 
    FROM {{ source('additional_data', 'products_info') }}
    ),

renamed_casted AS (
    SELECT
        CAST( NAME AS VARCHAR ) AS name
        , CAST( WEIGHT_KG AS FLOAT ) AS weight_kg
        , CAST( HEIGHT_CM AS FLOAT ) AS height_cm
        , CAST( LENGTH_CM AS FLOAT ) AS length_cm
        , CAST( WIDTH_CM AS FLOAT ) AS width_cm
    FROM src_products_info
    )

SELECT * FROM renamed_casted