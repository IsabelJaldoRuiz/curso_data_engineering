WITH src_employees AS (
    SELECT * 
    FROM {{ source('human_resources', 'employees') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['EMPLOYEE_ID', 'FIRST_NAME', 'LAST_NAME']) }} AS VARCHAR) AS employee_id
        , CAST( FIRST_NAME AS VARCHAR ) AS first_name
        , CAST( LAST_NAME AS VARCHAR ) AS last_name
        , CAST( GENDER AS VARCHAR ) AS gender
        , CAST( PRONOUNS AS VARCHAR ) AS pronouns
        , CAST( AGE AS NUMBER ) AS age
        , CAST( BIRTH_DATE AS DATE ) AS birth_date
        -- , CAST( JOB_TITLE AS VARCHAR ) AS job_title
        -- , CAST( DEPARTMENT AS VARCHAR ) AS department
        -- , CAST( BUSINESS_UNIT AS VARCHAR ) AS businnes_unit
        -- , CAST( JOB_LEVEL AS NUMBER ) AS job_level
        , CAST( 
            CASE CHARINDEX(',', LOCATION)
                WHEN 0 THEN LOCATION
                ELSE SUBSTRING(LOCATION, 1, CHARINDEX(',', LOCATION) - 1)
            END
                AS VARCHAR ) AS location
        -- , CAST( MANAGER_ID AS NUMBER ) AS number
        , CAST( BASE_SALARY AS NUMBER ) AS base_salary
        , CAST( BONUS_ELIGIBLE AS BOOLEAN ) AS is_bonus_eligible
        , CAST( BONUS_PCT AS FLOAT ) AS bonus_pct
        , CAST( EQUITY_GRANT AS BOOLEAN ) AS is_equity_grant
        , CAST( EQUITY_PCT AS FLOAT ) AS equity_ptc
        , CAST( EMPLOYMENT_TYPE AS VARCHAR ) AS employment_type
        , CAST( EMPLOYMENT_STATUS AS VARCHAR ) AS employment_status
        , CAST( HIRE_DATE AS DATE ) AS hire_date
        , CAST( TERMINATION_DATE AS DATE ) AS termination_date
        , CAST( ETHNICITY AS VARCHAR ) AS ethnicity
        , CAST( MARITAL_STATUS AS VARCHAR ) AS marital_status
        , CAST( EDUCATION_LEVEL AS VARCHAR ) AS education_level
        -- , CAST( VETERAN_STATUS AS BOOLEAN ) AS veteran_status
        -- , CAST( DISABILITY_STATUS AS BOOLEAN ) AS disability_status
        -- , CAST( COST_CENTER AS VARCHAR ) AS cost_center
        , CAST( FTE AS FLOAT ) AS fte
        , CAST( EXEMPTION_STATUS AS VARCHAR ) AS exemption_status
        , CAST( HIGH_POTENTIAL_FLAG AS BOOLEAN ) AS is_high_potential
        , CAST( SUCCESSION_PLAN_STATUS AS VARCHAR ) AS succession_plan_status
        , CAST( AIHR_CERTIFIED AS BOOLEAN ) AS is_aihr_certified
        , CAST( TRAINING_COUNT AS NUMBER ) AS training_count
        , CAST( LAST_TRAINING_DATE AS DATE ) AS last_training_date
        --, CAST( PROMOTION_COUNT AS NUMBER ) AS promotion_count
        --, CAST( LAST_PROMOTION_DATE AS DATE ) AS last_promotion_date
    FROM src_employees
    )

SELECT * FROM renamed_casted