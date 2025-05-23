SELECT *
FROM {{ ref('base_sql_server_dbo__orders') }}
WHERE delivered_at < created_at