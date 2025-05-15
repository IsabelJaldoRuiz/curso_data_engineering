WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS VARCHAR ) AS user_id 
        --, user_id AS old_user_id
        , updated_at
        , CAST( {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS VARCHAR ) AS address_id
        , CAST( first_name AS VARCHAR ) AS first_name
        , CAST( last_name AS VARCHAR ) AS last_name
        , created_at
        , phone_number
        -- , total_orders
        , CAST( email AS VARCHAR ) AS email
        , CAST( IFNULL (_fivetran_deleted, FALSE) AS BOOLEAN ) AS is_deleted
        , CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) AS date_load
    FROM src_users
    )

SELECT * FROM renamed_casted