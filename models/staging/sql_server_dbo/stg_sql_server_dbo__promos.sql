{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    
    UNION ALL
    
    SELECT 
        'non promo' AS promo_id
        , 0 AS discount
        , 'inactive' AS status
        , NULL AS _fivetran_deleted
        , CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ) AS _fivetran_synced
    ),

stg_promos AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS VARCHAR ) AS promo_id 
        , CAST( LOWER( REPLACE( promo_id, '-', ' ' ) ) AS VARCHAR ) AS promo_desc
        , CAST( discount AS NUMBER ) AS discount
        , CAST( status AS VARCHAR ) AS status
        , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_promos
    )

SELECT * FROM stg_promos