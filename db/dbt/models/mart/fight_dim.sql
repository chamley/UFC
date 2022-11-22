


{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('fight_stg') }}