WITH stg_job_possitions AS (
    SELECT * 
    FROM {{ ref('stg_human_resources__job_possitions') }}
    ),

dim_job_possitions AS (
    SELECT
        job_possition_id
        , job_title
        , department
        , businness_unit
    FROM stg_job_possitions
    )

SELECT * FROM dim_job_possitions