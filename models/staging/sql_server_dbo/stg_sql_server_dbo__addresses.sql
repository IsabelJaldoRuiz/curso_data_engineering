{{
  config(
    materialized='view'
  )
}}

WITH src_addresses AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__addresses') }}
    ),

renamed_casted AS (
    SELECT
        address_id 
        , address_number
        , address
        , country
        , state
        , zipcode
        , is_deleted
        , date_load 
    FROM src_addresses
    )

SELECT * FROM renamed_casted