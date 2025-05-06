WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
        {{ dbt.hash('user_id') }} AS user_id 
        , user_id AS old_user_id
        , updated_at
        , address_id
        , last_name
        , created_at
        , phone_number
        -- , total_orders
        , first_name
        , email
        , _fivetran_deleted AS is_deleted
        , CONVERT_TIMEZONE('UTC', _fivetran_synced::TIMESTAMP_TZ(9)) AS date_load
    FROM src_users
    )

SELECT * FROM renamed_casted