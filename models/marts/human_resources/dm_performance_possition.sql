-- Desempeño promedio por cargo, por trimestre

WITH fct_with_dims AS (
    SELECT 
        jp.job_title
        , hr.performance_rating
        , hr.engagement_score
        , date.year_number AS year
        , date.quarter_of_year AS quarter
    FROM {{ ref('fct_hr') }} AS hr
    INNER JOIN {{ ref('dim_employees') }} AS emp
        ON hr.employee_id = emp.employee_id
    INNER JOIN {{ ref('dim_job_possitions') }} AS jp
        ON hr.job_possition_id = jp.job_possition_id
    INNER JOIN {{ ref('dim_date') }} AS date
        ON hr.shapshot_date = date.date_day
),

dm_performance_position AS (
    SELECT 
        job_title
        , year
        , quarter
        , AVG( performance_rating ) AS avg_performance
        , AVG( engagement_score ) AS avg_engagement
    FROM fct_with_dims
    GROUP BY job_title, year, quarter
    )

SELECT * FROM dm_performance_position