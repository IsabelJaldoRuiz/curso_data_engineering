WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['product_id', 'month']) }} AS VARCHAR ) AS budget_id 
        -- , _row
        -- , CAST(product_id AS VARCHAR) AS old_product_id
        , CAST(month AS DATE) AS month
        , CAST(quantity AS NUMBER) AS quantity
        , CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) AS date_load
    FROM src_budget
    )

SELECT * FROM renamed_casted