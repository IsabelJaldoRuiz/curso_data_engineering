WITH src_addresses_info AS (
    SELECT * 
    FROM {{ source('additional_data', 'addresses_info') }}
    ),

renamed_casted AS (
    SELECT
        CAST( ZIPCODE AS VARCHAR ) AS zipcode
        , CAST( LATITUDE AS FLOAT ) AS latitude
        , CAST( LONGITUDE AS FLOAT ) AS longitude
        , CAST( POPULATION AS NUMBER ) AS population
        , CAST( DENSITY AS FLOAT ) AS density
        , CAST( PERCENTAGE_MALE AS FLOAT ) AS percent_male
        , CAST( PERCENTAGE_FEMALE AS FLOAT ) AS percent_female
        , CAST( AVERAGE_AGE AS FLOAT ) AS average_age
        , CAST( PERCENTAGE_LESS_18_YEARS AS FLOAT ) AS percent_less_18_years
        , CAST( PERCENTAGE_18_64_YEARS AS FLOAT ) AS percent_18_64_years
        , CAST( PERCENTAGE_65_MORE_YEARS AS FLOAT ) AS percent_65_more_years
    FROM src_addresses_info
    )

SELECT * FROM renamed_casted