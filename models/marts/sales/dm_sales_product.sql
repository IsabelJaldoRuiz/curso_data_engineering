WITH fct_with_dims AS (
    SELECT 
        o.order_id
        , oi.product_id
        , p.name AS product_name
        , d.year_number
        , d.month_of_year
        ,oi.quantity * oi.price AS total_sales
    FROM {{ ref('fct_orders') }} AS o
    INNER JOIN {{ ref('dim_order_items') }} AS oi ON o.order_id = oi.order_id
    INNER JOIN {{ ref('dim_products') }} AS p ON oi.product_id = p.product_id
    INNER JOIN {{ ref('dim_date') }} AS d ON o.created_at_date = d.date_day
), 

dm_sales_product AS (
    SELECT 
        product_name
        , year_number
        , month_of_year
        , SUM( total_sales ) AS total_sales
    FROM fct_with_dims
    GROUP BY product_name, year_number, month_of_year
    ORDER BY year_number, month_of_year
)

SELECT * FROM dm_sales_product