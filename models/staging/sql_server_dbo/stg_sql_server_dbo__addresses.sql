{{
  config(
    materialized='view'
  )
}}

WITH bases_addresses_addresses_info AS (
    SELECT addresess.* 
        , latitude
        , longitude
        , population
        , density
        , percent_male
        , percent_female
        , average_age
        , percent_less_18_years
        , percent_18_64_years
        , percent_65_more_years
    FROM {{ ref('base_sql_server_dbo__addresses') }} AS addresess
    INNER JOIN
    {{ ref('base_additional_data__addresses_info') }} AS address_info
    ON addresess.zipcode = address_info.zipcode
    ),

stg_addresses AS (
    SELECT
        address_id 
        , address
        , address_number
        , state
        , country
        , zipcode
        , latitude
        , longitude
        , population
        , density
        , percent_male
        , percent_female
        , average_age
        , percent_less_18_years
        , percent_18_64_years
        , percent_65_more_years
        , is_deleted
        , date_load 
    FROM bases_addresses_addresses_info
    )

SELECT * FROM stg_addresses