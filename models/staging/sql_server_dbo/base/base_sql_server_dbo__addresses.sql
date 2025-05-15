WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS VARCHAR) AS address_id 
        -- , CAST( address_id AS VARCHAR ) AS old_address_id
        , CAST( SUBSTRING(address, 0, CHARINDEX(' ', address, 0) - 1) AS NUMBER ) AS address_number
        , CAST( SUBSTRING(address, CHARINDEX(' ', address, 0) + 1, LEN(address)) AS VARCHAR ) AS address
        , CAST( country AS VARCHAR ) AS country
        , CAST( state AS VARCHAR ) AS state
        , LPAD(CAST(zipcode AS VARCHAR), 5, '0') AS zipcode
        , CAST( IFNULL (_fivetran_deleted, FALSE) AS BOOLEAN ) AS is_deleted
        , CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) AS date_load
    FROM src_addresses
    )

SELECT * FROM renamed_casted