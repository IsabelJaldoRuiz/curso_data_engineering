WITH fct_with_dims AS (
    SELECT 
        orders.order_id
        , orders.estimated_delivery_at_date
        , orders.delivered_at_date
        , addresses.state
    FROM {{ ref('fct_orders') }} AS orders
    INNER JOIN {{ ref('dim_addresses') }} AS addresses
        ON orders.address_id = addresses.address_id
    INNER JOIN {{ ref('dim_date') }} AS date
        ON orders.delivered_at_date = date.date_day
    WHERE orders.status = 'delivered'
        AND orders.estimated_delivery_at_date IS NOT NULL
        AND orders.delivered_at_date IS NOT NULL
),

dm_delivery AS (
    SELECT
        order_id
        , estimated_delivery_at_date
        , delivered_at_date
        , DATEDIFF(day, delivered_at_date, estimated_delivery_at_date) AS days_delay
        , state
        , CASE
            WHEN DATEDIFF(day, delivered_at_date, estimated_delivery_at_date) > 0 THEN 'Early'
            WHEN DATEDIFF(day, delivered_at_date, estimated_delivery_at_date) < 0 THEN 'Late'
            ELSE 'On Time'
        END AS delivery_status
    FROM fct_with_dims
)

SELECT * FROM dm_delivery