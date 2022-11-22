


{{ config(materialized="table") }}

with data as (select * 
from {{ ref('fight_stg') }})

select red_fighter_name fighter_name, red_fighter_id fighter_id
from data
union
select blue_fighter_name fighter_name, blue_fighter_id fighter_id
from data




