WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
        {{ dbt.hash('address_id') }} AS address_id 
        , address_id AS old_address_id
        , address
        , country
        , state
        , zipcode
        , _fivetran_deleted AS is_deleted
        , CONVERT_TIMEZONE('UTC', _fivetran_synced::TIMESTAMP_TZ(9)) AS date_load
    FROM src_addresses
    )

SELECT * FROM renamed_casted