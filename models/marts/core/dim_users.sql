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
        , email
        , address_id
        , created_at
        , updated_at
    FROM stg_users
    )

SELECT * FROM dim_users