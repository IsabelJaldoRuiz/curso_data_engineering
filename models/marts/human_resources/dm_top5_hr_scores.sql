-- Top 5 ranking de cada mes de los empleados con mayor desempeño, compromiso y riesgo de abandono

WITH fct_with_dims AS (
    SELECT 
        hr.employee_id
        , hr.performance_rating
        , hr.engagement_score
        , hr.risk_of_exit_score
        , emp.first_name
        , emp.last_name
        , date.month_of_year AS month
        , date.quarter_of_year AS quarter
        , date.year_number AS year
    FROM {{ ref('fct_hr') }} AS hr
    INNER JOIN {{ ref('dim_employees') }} AS emp
    ON hr.employee_id = emp.employee_id
    INNER JOIN {{ ref('dim_date') }} AS date
    ON hr.shapshot_date = date.date_day
    ),

ranked AS (
    SELECT *
        , RANK() OVER (PARTITION BY year, month ORDER BY performance_rating DESC) AS rank_perf
        , RANK() OVER (PARTITION BY year, month ORDER BY engagement_score DESC) AS rank_engagement
        , RANK() OVER (PARTITION BY year, month ORDER BY risk_of_exit_score DESC) AS rank_risk
    FROM fct_with_dims
),

dm_top5_hr_scores AS (
    SELECT
        year
        , month
        , quarter
        , employee_id
        , first_name
        , last_name
        , performance_rating
        , engagement_score
        , risk_of_exit_score
        , rank_perf
        , rank_engagement
        , rank_risk
    FROM ranked
    WHERE 
        rank_perf <= 5 OR 
        rank_engagement <= 5 OR 
        rank_risk <= 5
    )

SELECT * FROM dm_top5_hr_scores