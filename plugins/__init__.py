from airflow.plugins_manager import AirflowPlugin
from operators.custom_airbyte_operator import CustomAirbyteOperator

class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [CustomAirbyteOperator]
