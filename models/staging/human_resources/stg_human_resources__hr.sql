WITH base_employees_snapshots AS (
    SELECT * 
    FROM {{ ref('base_human_resources__employees_snapshots') }}
    ),

stg_hr AS (
    SELECT
        snapshot_id
        , employee_id
        , CAST( {{ dbt_utils.generate_surrogate_key(['job_title']) }} AS VARCHAR ) AS job_possition_id
        , training_count
        , last_training_date
        , promotion_count
        , last_promotion_date
        , shapshot_date
        , tenure_months 
        , performance_rating
        , current_salary
	    , engagement_score
	    , risk_of_exit_score
    FROM base_employees_snapshots
    )

SELECT * FROM stg_hr