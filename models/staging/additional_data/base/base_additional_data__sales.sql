WITH src_sales AS (
    SELECT * 
    FROM {{ source('additional_data', 'sales') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['sale_id']) }} AS VARCHAR) AS order_id 
        , CAST( {{ dbt_utils.generate_surrogate_key(['employee_id', 'first_name', 'last_name']) }} AS VARCHAR) AS employee_id
        , CONVERT_TIMEZONE('UTC', CAST( sold_at AS TIMESTAMP_TZ )) AS created_at 
        , CAST( {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS VARCHAR ) AS promo_id
        , CAST( order_cost AS FLOAT) AS order_cost
        , CAST( order_total AS FLOAT ) AS order_total
         , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_sales
    )

SELECT * FROM renamed_casted