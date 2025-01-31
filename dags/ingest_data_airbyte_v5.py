from airflow import DAG
from datetime import datetime, timedelta
from operators.custom_airbyte_operator import CustomAirbyteOperator, WaitForAirbyteSyncOperator
import os
from dotenv import load_dotenv
from airflow.utils.task_group import TaskGroup
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt_airflow.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG

# Load environment variables
load_dotenv()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ingest_data_airbyte_19_conns",
    default_args=default_args,
    start_date=datetime(2024, 11, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=['airbyte', 'airflow', 'bigquery']
) as dag:

    # TaskGroup untuk sinkronisasi CSV ke BigQuery
    with TaskGroup("csv_to_bigquery_syncs", tooltip="Sync CSV to BigQuery") as csv_to_bigquery_group:

        connections = {
            'inventory_transactions': 'HTTP_PUBLIC_CSV_INVENTORY_TRANSACTIONS',
            'order_details': 'HTTP_PUBLIC_CSV_ORDER_DETAILS',
            'orders': 'HTTP_PUBLIC_CSV_ORDERS',
            'products': 'HTTP_PUBLIC_CSV_PRODUCTS',
            'suppliers': 'HTTP_PUBLIC_CSV_SUPPLIERS',
            'privileges': 'HTTP_PUBLIC_CSV_PRIVILEGES',
            'purchase_order_status': 'HTTP_PUBLIC_CSV_PURCHASE_ORDER_STATUS',
            'customer': 'HTTP_PUBLIC_CSV_CUSTOMER',
            'order_details_status': 'HTTP_PUBLIC_CSV_ORDER_DETAILS_STATUS',
            'purchase_orders': 'HTTP_PUBLIC_CSV_PURCHASE_ORDERS',
            'invoices': 'HTTP_PUBLIC_CSV_INVOICES',
            'purchase_order_details': 'HTTP_PUBLIC_CSV_PURCHASE_ORDER_DETAILS',
            'strings': 'HTTP_PUBLIC_CSV_STRINGS',
            'inventory_transaction_types': 'HTTP_PUBLIC_CSV_INVENTORY_TRANSACTION_TYPES',
            'orders_status': 'HTTP_PUBLIC_CSV_ORDERS_STATUS',
            'shippers': 'HTTP_PUBLIC_CSV_SHIPPERS',
            'orders_tax_status': 'HTTP_PUBLIC_CSV_ORDERS_TAX_STATUS',
            'employees': 'HTTP_PUBLIC_CSV_EMPLOYEES',
            'employee_privileges': 'HTTP_PUBLIC_CSV_EMPLOYEE_PRIVILEGES',
        }

        tasks = {}

        for task_name, env_var in connections.items():
            tasks[task_name] = CustomAirbyteOperator(
                task_id=f'ingest_csv_to_bigquery_{task_name}',
                airbyte_url=os.getenv('AIRBYTE_URL'),
                connection_id=os.getenv(env_var),
                api_key=os.getenv('AIRBYTE_API_KEY')
            )

        # Chain tasks dynamically
        previous_task = None
        for task in tasks.values():
            if previous_task:
                previous_task >> task
            previous_task = task

    # WaitForAirbyteSyncOperator untuk menunggu semua sinkronisasi selesai
    wait_for_sync = WaitForAirbyteSyncOperator(
        task_id='wait_for_sync',
        airbyte_url=os.getenv("AIRBYTE_URL"),
        connection_ids=[os.getenv(env_var) for env_var in connections.values()],
        api_key=os.getenv("AIRBYTE_API_KEY"),
        polling_interval=30,  # Poll setiap 30 detik
        timeout=3600  # Timeout setelah 1 jam
    )

    # DBT Task Group
    northwind_data = DbtTaskGroup(
        group_id="dbtTaskGroup",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models']
        )
    )

    # Dependency chain
    csv_to_bigquery_group >> wait_for_sync >> northwind_data
