WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

dim_shipping_service AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_id
        , shipping_service
    FROM stg_orders
    GROUP BY shipping_service
    )

SELECT * FROM dim_shipping_service