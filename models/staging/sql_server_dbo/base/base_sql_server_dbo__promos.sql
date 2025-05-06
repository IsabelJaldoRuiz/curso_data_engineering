WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    )
        
    UNION ALL
    
    SELECT 
        "non-promo" AS promo_id
        , 0 AS discount
        , "inactive" AS status
        , NULL AS _fivetran_deleted
        , SYSDATE() AS _fivetran_synced
    ,


renamed_casted AS (
    SELECT
        {{ dbt.hash('promo_id') }} AS promo_id 
        , promo_id AS promo_desc
        , discount
        , status
        , _fivetran_deleted AS is_deleted
        , CONVERT_TIMEZONE('UTC', _fivetran_synced::TIMESTAMP_TZ(9)) AS date_load
    FROM src_promos
    )


SELECT * FROM renamed_casted