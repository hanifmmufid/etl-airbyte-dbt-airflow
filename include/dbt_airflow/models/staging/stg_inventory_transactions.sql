with source as (

    select * from {{ source('my_project_dbt_3','raw_inventory_transactions') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source