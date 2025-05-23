WITH src_employees_snapshots AS (
    SELECT * 
    FROM {{ ref('base_human_resources__employees_snapshots') }}
    ),

renamed_casted AS (
    SELECT DISTINCT
        CAST( {{ dbt_utils.generate_surrogate_key(['JOB_TITLE']) }} AS VARCHAR ) AS job_possition_id
        , job_title
        , department
        , business_unit
    FROM src_employees_snapshots
    )

SELECT * FROM renamed_casted