-- Promociones por puesto y año

WITH fct_with_dims AS (
    SELECT 
        jp.job_title
        , hr.promotion_count
        , date.year_number AS year
    FROM {{ ref('fct_hr') }} AS hr
    INNER JOIN {{ ref('dim_employees') }} AS emp
        ON hr.employee_id = emp.employee_id
    INNER JOIN {{ ref('dim_job_possitions') }} AS jp
        ON hr.job_possition_id = jp.job_possition_id
    INNER JOIN {{ ref('dim_date') }} AS date
        ON hr.shapshot_date = date.date_day
),

dm_promotions AS (
    SELECT 
        job_title
        , year
        , SUM(promotion_count) AS total_promotions
    FROM fct_with_dims
    GROUP BY job_title, year
)

SELECT * FROM dm_promotions