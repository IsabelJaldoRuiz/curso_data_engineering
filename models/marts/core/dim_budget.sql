WITH stg_budget AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets__budget') }}
    ),

dim_budget AS (
    SELECT
        budget_id 
        , month
        , quantity
    FROM stg_budget
    )

SELECT * FROM dim_budget