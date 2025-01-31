with source as (

    select * from {{ source('my_project_dbt_3','raw_purchase_order_details') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source