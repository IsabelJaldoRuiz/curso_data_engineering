WITH src_employees_snapshots AS (
    SELECT * 
    FROM {{ source('human_resources', 'employees_snapshots') }}
    ),

renamed_casted AS (
    SELECT DISTINCT
        CAST( {{ dbt_utils.generate_surrogate_key(['JOB_TITLE']) }} AS VARCHAR ) AS job_possition_id
        , CAST( JOB_TITLE AS VARCHAR ) AS job_title
        , CAST( DEPARTMENT AS VARCHAR ) AS department
        , CAST( BUSINESS_UNIT AS VARCHAR ) AS businness_unit
    FROM src_employees_snapshots
    )

SELECT * FROM renamed_casted