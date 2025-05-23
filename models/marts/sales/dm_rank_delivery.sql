WITH fct_with_dims AS (
    SELECT 
        orders.order_id
        , orders.estimated_delivery_at_date
        , orders.delivered_at_date
        , addresses.state
    FROM {{ ref('fct_orders') }} AS orders
    INNER JOIN {{ ref('dim_addresses') }} AS addresses
        ON orders.address_id = addresses.address_id
    WHERE orders.status = 'delivered'
        AND orders.estimated_delivery_at_date IS NOT NULL
        AND orders.delivered_at_date IS NOT NULL
),

dm_delivery_delays AS (
    SELECT
        state
        , AVG(DATEDIFF(day, delivered_at_date, estimated_delivery_at_date)) AS avg_days_delay
    FROM fct_with_dims
    GROUP BY state
),

ranked_delays AS (
    SELECT *
        , ROW_NUMBER() OVER (ORDER BY avg_days_delay DESC) AS rank_late
        , ROW_NUMBER() OVER (ORDER BY avg_days_delay ASC) AS rank_early
    FROM dm_delivery_delays
),

top_states AS (
    SELECT *
    FROM ranked_delays
    WHERE rank_late <= 5 OR rank_early <= 5
)

SELECT 
    state
    , avg_days_delay
    , rank_late
    , rank_early
FROM top_states
ORDER BY avg_days_delay DESC