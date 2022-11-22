
-- Use the `ref` function to select from other models

select *
from {{ source('ufcstats_source', 'fight_source') }}