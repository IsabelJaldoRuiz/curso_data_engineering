{% macro get_fivetran_fiels(_fivetran_deleted, _fivetran_synced) %}
    {% set is_deleted = CAST( IFNULL (_fivetran_deleted, FALSE) AS BOOLEAN ) %}
    {% set date_load = CONVERT_TIMEZONE('UTC', CAST(_fivetran_synced AS TIMESTAMP_TZ)) %}
    {{ return(is_deleted, date_load) }}
{% endmacro %}