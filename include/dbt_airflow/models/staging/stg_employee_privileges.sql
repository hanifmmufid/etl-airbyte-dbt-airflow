with source as (

    select * from {{ source('my_project_dbt_3','raw_employee_privileges') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source