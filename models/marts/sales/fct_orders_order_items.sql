WITH stg_orders_order_items AS (
    SELECT orders.*
        , product_id
        , quantity
        , price
    FROM  {{ ref('dim_order_items') }} AS order_items
    LEFT JOIN {{ ref('fct_orders') }} AS orders
    ON orders.order_id = order_items.order_id
),

fct_orders_order_items AS (
    SELECT
        order_id 
        , shipping_service_id
        , {{ dbt_utils.generate_surrogate_key(['shipping_cost', 'order_cost', 'order_total']) }} AS order_costs_id
        , address_id
        , created_at 
        , promo_id
        , estimated_delivery_at
        , user_id
        , delivered_at
        , tracking_id
        , status
        , product_id
        , quantity
        , price
    FROM stg_orders_order_items
    )

SELECT * FROM fct_orders_order_items