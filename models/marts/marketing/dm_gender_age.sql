-- Segmentación clientes según su estado por género, grupo de edad y edad media

WITH fct_orders_dim_addresses AS (
    SELECT 
        orders.*
        , state
        , percent_male 
        , percent_female
        , average_age
        , percent_less_18_years
        , percent_18_64_years
        , percent_65_more_years 
    FROM  {{ ref('fct_orders') }} AS orders
    INNER JOIN
        {{ ref('dim_addresses') }} AS addresses
    ON orders.address_id = addresses.address_id
),

mart_gender_age AS (
    SELECT
        state
        , CASE
            WHEN MAX( percent_male ) > MAX( percent_female ) THEN 'male'
            WHEN MAX( percent_male ) < MAX( percent_female ) THEN 'female'
            ELSE 'equals'
        END AS gender_likely
        , CASE
            WHEN MAX( percent_less_18_years ) > MAX( percent_18_64_years ) THEN 
                CASE
                    WHEN MAX( percent_less_18_years ) > MAX( percent_18_64_years ) THEN 'less 18 years'
                    ELSE '18-64 years'
                END
            WHEN MAX( percent_less_18_years ) < MAX( percent_18_64_years ) THEN
                CASE
                    WHEN MAX( percent_18_64_years ) > MAX( percent_65_more_years ) THEN '18-64 years'
                    ELSE 'more 65 years'
                END
        END AS age_likely
        , MAX( average_age ) AS average_age
    FROM fct_orders_dim_addresses
    GROUP BY state
    )

SELECT * FROM mart_gender_age