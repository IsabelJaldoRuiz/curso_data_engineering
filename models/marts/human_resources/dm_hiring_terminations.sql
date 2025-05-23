-- Contrataciones y finalizaciones del contrato, mensuales

WITH date_dim AS (
    SELECT 
        date_day,
        year_number,
        month_of_year,
        MIN(date_day) OVER (PARTITION BY year_number, month_of_year) AS month_start,
        MAX(date_day) OVER (PARTITION BY year_number, month_of_year) AS month_end
    FROM {{ ref('dim_date') }}
),

dm_hiring_terminations AS (
    SELECT 
        date.year_number AS year
        , date.month_of_year AS month
        , COUNT(DISTINCT CASE WHEN emp.hire_date BETWEEN date.month_start AND date.month_end THEN emp.employee_id END) AS hires
        , COUNT(DISTINCT CASE WHEN emp.termination_date BETWEEN date.month_start AND date.month_end THEN emp.employee_id END) AS terminations
    FROM {{ ref('dim_employees') }} AS emp
    INNER JOIN date_dim AS date
        ON emp.hire_date BETWEEN date.month_start AND date.month_end 
            OR emp.termination_date BETWEEN date.month_start AND date.month_end
    GROUP BY date.year_number, date.month_of_year
)

SELECT * FROM dm_hiring_terminations
ORDER BY year, month