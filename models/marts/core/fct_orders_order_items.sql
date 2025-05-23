{{
    config(
        materialized = "incremental",
        unique_key = "created_at_date"
    )
}}

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
        , created_at_date
        , promo_id
        , estimated_delivery_at_date
        , user_id
        , delivered_at_date
        , tracking_id
        , status
        , product_id
        , quantity
        , price
        , DATEDIFF(day, delivered_at_date, estimated_delivery_at_date)
    FROM stg_orders_order_items
    )

SELECT * FROM fct_orders_order_items

{% if is_incremental() %}
	  WHERE date_load > (SELECT MAX(date_load) FROM {{ this }} )
{% endif %}