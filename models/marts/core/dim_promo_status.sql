WITH stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__promos') }}
    ),

dim_promo_status AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_id
        , status
    FROM stg_promos
    GROUP BY status
    )

SELECT * FROM dim_promo_status