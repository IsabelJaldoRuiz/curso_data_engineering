-- Tendencias mensuales y trimestrales de métricas de RR.HH.
-- Salario promedio, rendimiento y satisfacción del personal, riesgo de fuga de talento y cantidad de empleados activos

WITH fct_with_dims AS (
    SELECT 
        hr.employee_id
        , hr.shapshot_date
        , hr.performance_rating
        , hr.current_salary
        , hr.engagement_score
        , hr.risk_of_exit_score
        , date.month_of_year AS month
        , date.quarter_of_year AS quarter
        , date. year_number AS year
    FROM {{ ref('fct_hr') }} AS hr
    INNER JOIN {{ ref('dim_date') }} AS date
    ON hr.shapshot_date = date.date_day
    ),

dm_avg_hr_metrics AS (
    SELECT
        year,
        month,
        quarter,
        AVG(current_salary) AS avg_salary,
        AVG(performance_rating) AS avg_performance_rating,
        AVG(engagement_score) AS avg_engagement_score,
        AVG(risk_of_exit_score) AS avg_risk_of_exit_score,
        COUNT(DISTINCT employee_id) AS employee_count
    FROM fct_with_dims
    GROUP BY year, month, quarter
    )

SELECT * FROM dm_avg_hr_metrics