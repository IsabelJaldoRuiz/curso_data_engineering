{{
    config(
        materialized = "incremental",
        unique_key = "created_at_date"
    )
}}

WITH stg_orders AS (
    SELECT * 
    FROM  {{ ref('stg_sql_server_dbo__orders') }}
),

fct_orders AS (
    SELECT
        order_id 
        , employee_id
        , shipping_service_id
        , shipping_cost
        , address_id
        , DATE( created_at ) AS created_at_date
        , TIME( created_at ) AS created_at_time
        , promo_id
        , DATE( estimated_delivery_at ) AS estimated_delivery_at_date
        , TIME( estimated_delivery_at ) AS estimated_delivery_at_time
        , order_cost
        , user_id
        , order_total
        , DATE( delivered_at ) AS delivered_at_date
        , TIME( delivered_at ) AS delivered_at_time
        , tracking_id
        , status
    FROM stg_orders
    )

SELECT * FROM fct_orders

{% if is_incremental() %}
	  WHERE date_load > (SELECT MAX(date_load) FROM {{ this }} )
{% endif %}