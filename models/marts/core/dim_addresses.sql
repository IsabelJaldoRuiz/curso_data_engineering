WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
    ),

dim_addresses AS (
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
    FROM stg_addresses
    )

SELECT * FROM dim_addresses