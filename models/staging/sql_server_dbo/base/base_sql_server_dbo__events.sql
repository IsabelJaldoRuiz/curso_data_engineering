WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
        {{ dbt.hash('event_id') }} AS event_id 
        , event_id AS old_event_id
        , page_url
        , event_type
        , user_id
        , product_id
        , session_id
        , created_at
        , _fivetran_deleted AS is_deleted
        , CONVERT_TIMEZONE('UTC', _fivetran_synced::TIMESTAMP_TZ(9)) AS date_load
    FROM src_events
    )

SELECT * FROM renamed_casted