WITH fct_events AS (
    SELECT
        e.event_id
        , et.event_type
        , p.name
        , e.user_id
        , e.session_id
        , e.created_at_date
    FROM {{ ref('fct_events') }} AS e
    INNER JOIN {{ ref('dim_event_type') }} AS et 
    ON e.event_type_id = et.event_type_id
    INNER JOIN {{ ref('dim_products') }} AS p
    ON e.product_id = p.product_id
    WHERE e.product_id IS NOT NULL
),

product_event_counts AS (
    SELECT
        name
        , event_type
        , COUNT(*) AS event_count
        , COUNT(DISTINCT user_id) AS unique_users
    FROM fct_events
    GROUP BY name, event_type
)

SELECT *
FROM product_event_counts
ORDER BY event_count DESC