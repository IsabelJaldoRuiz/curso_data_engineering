{{ 
    config(
        materialized='incremental',
        unique_key = 'order_id',
        on_schema_change='fail'
    ) 
}}

WITH bases_order_sale_items AS (
    SELECT
        order_id 
        , NULL as employee_id
        , shipping_service
        , shipping_service_id 
        , shipping_cost
        , address_id
        , created_at 
        , promo_id
        , estimated_delivery_at
        , order_cost
        , user_id
        , order_total
        , delivered_at
        , tracking_id
        , status
        , is_deleted
        , date_load 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    
    UNION ALL
    
    SELECT
        order_id 
        , employee_id
        , NULL AS shipping_service
        , NULL AS shipping_service_id 
        , NULL AS shipping_cost
        , NULL AS address_id
        , created_at 
        , NULL AS promo_id
        , NULL AS estimated_delivery_at
        , order_cost
        , NULL AS user_id
        , order_total
        , NULL AS delivered_at
        , NULL AS tracking_id
        , NULL AS status
        , is_deleted
        , date_load 
    FROM {{ ref('base_additional_data__sales') }}
    ), 

stg_orders AS (
    SELECT
        order_id 
        , employee_id
        , shipping_service
        , shipping_service_id 
        , shipping_cost
        , address_id
        , created_at 
        , promo_id
        , estimated_delivery_at
        , order_cost
        , user_id
        , order_total
        , delivered_at
        , tracking_id
        , status
        , is_deleted
        , date_load

    FROM bases_order_sale_items
)

SELECT * FROM stg_orders

{% if is_incremental() %}
	  WHERE date_load > (SELECT MAX(date_load) FROM {{ this }} )
{% endif %}