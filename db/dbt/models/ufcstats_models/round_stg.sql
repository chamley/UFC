
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{ config(materialized="incremental") }}

 select * from{{ source('ufcstats_source', 'round_source') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where loaded_at > (select max(loaded_at) from {{ this }}) 
{% endif %}