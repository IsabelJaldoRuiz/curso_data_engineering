WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__users') }}
    ),

dim_users AS (
    SELECT
        user_id 
        , first_name
        , last_name
        , phone_number
        , is_phone_number_valid
        , email
        , is_email_valid
        , address_id
        , created_at
        , updated_at
    FROM stg_users
    )

SELECT * FROM dim_users