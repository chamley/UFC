


{{ config(materialized="table") }}

with data as (select * 
from {{ ref('round_stg') }})