WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS VARCHAR) AS order_id 
        -- , order_id AS old_order_id
        , CAST ( CASE shipping_service
            WHEN '' THEN 'non-service'
            ELSE shipping_service
        END AS VARCHAR) AS shipping_service
        , CAST( shipping_cost AS FLOAT ) AS shipping_cost
        , CAST( {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS VARCHAR ) AS address_id
        , CONVERT_TIMEZONE('UTC', CAST( created_at AS TIMESTAMP_TZ )) AS created_at 
        , CASE promo_id
            WHEN '' THEN {{ dbt_utils.generate_surrogate_key(["'non-promo'"]) }}
            ELSE {{ dbt_utils.generate_surrogate_key(['promo_id']) }}
        END AS promo_id
        , CONVERT_TIMEZONE('UTC', CAST( estimated_delivery_at AS TIMESTAMP_TZ )) AS estimated_delivery_at
        , CAST( order_cost AS FLOAT) AS order_cost
        , CAST( {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS VARCHAR ) AS user_id
        , CAST( order_total AS FLOAT ) AS order_total
        , CONVERT_TIMEZONE('UTC', CAST(delivered_at AS TIMESTAMP_TZ)) AS delivered_at
        , CAST( {{ dbt_utils.generate_surrogate_key(['tracking_id']) }} AS VARCHAR ) AS tracking_id
        , CAST( status AS VARCHAR ) AS status
        , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_orders
    )

SELECT * FROM renamed_casted