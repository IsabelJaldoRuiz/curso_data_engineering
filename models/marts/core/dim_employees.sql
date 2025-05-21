WITH stg_employees AS (
    SELECT * 
    FROM {{ ref('stg_human_resources__employees') }}
    ),

dim_employees AS (
    SELECT
        employee_id
        , first_name
        , last_name
        , gender
        , pronouns
        , age
        , birth_date
        , location
        , base_salary
        , is_bonus_eligible
        , bonus_pct
        , is_equity_grant
        , equity_ptc
        , employment_type
        , employment_status
        , hire_date
        , termination_date
        , ethnicity
        , marital_status
        , education_level
        , fte
        , exemption_status
        , is_high_potential
        , succession_plan_status
        , is_aihr_certified
    FROM stg_employees
    )

SELECT * FROM dim_employees