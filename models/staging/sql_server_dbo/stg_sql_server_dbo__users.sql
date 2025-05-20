WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS VARCHAR ) AS user_id 
        --, user_id AS old_user_id
        , CAST( first_name AS VARCHAR ) AS first_name
        , CAST( last_name AS VARCHAR ) AS last_name
        , CAST( phone_number AS VARCHAR ) AS phone_number
        , CAST( email AS VARCHAR ) AS email
        , CAST( {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS VARCHAR ) AS address_id
        , CONVERT_TIMEZONE('UTC', CAST( created_at AS TIMESTAMP_TZ )) AS created_at
        , CONVERT_TIMEZONE(
            'UTC',
            CAST(IFNULL(
                    updated_at,
                    CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ)
                    ) AS TIMESTAMP_TZ
                )
            ) AS updated_at
        -- , total_orders
        , {{ add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) }} 
    FROM src_users
    )

SELECT * FROM stg_users