WITH stg_hr AS (
    SELECT * 
    FROM {{ ref('stg_human_resources__hr') }}
    ),

fct_hr AS (
    SELECT
        snapshot_id
        , employee_id
        , job_possition_id
        , promotion_count
        , last_promotion_date
        , shapshot_date
        , tenure_months 
        , performance_rating
        , current_salary
	    , engagement_score
	    , risk_of_exit_score
    FROM stg_hr
    )

SELECT * FROM fct_hr