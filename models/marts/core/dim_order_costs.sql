WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

dim_order_costs AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['shipping_cost', 'order_cost', 'order_total']) }} AS order_costs_id
        , shipping_cost
        , order_cost
        , order_total
    FROM stg_orders
    )

SELECT * FROM dim_order_costs