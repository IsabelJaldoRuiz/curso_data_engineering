WITH stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__promos') }}
    ),

dim_promos AS (
    SELECT
        promo_id 
        , promo_desc
        , discount
        , {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_id
    FROM stg_promos
    )

SELECT * FROM dim_promos