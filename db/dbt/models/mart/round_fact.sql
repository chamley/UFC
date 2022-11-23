


{{
    config(materialized='table')
}}


select * from {{ ref('round_stg') }}