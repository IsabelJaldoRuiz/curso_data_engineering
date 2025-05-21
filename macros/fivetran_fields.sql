{% macro add_fivetran_metadata(_fivetran_deleted, _fivetran_synced) %}
    CAST( IFNULL (_fivetran_deleted, FALSE) AS BOOLEAN ) AS is_deleted
    , CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) AS date_load
{% endmacro %}