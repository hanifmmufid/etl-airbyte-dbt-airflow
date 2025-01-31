# di file operators/custom_airbyte_operator.py

from airflow.models import BaseOperator
import requests
from airflow.utils.decorators import apply_defaults
import time

class CustomAirbyteOperator(BaseOperator):
    def __init__(
        self,
        connection_id: str,
        airbyte_url: str = "http://localhost:8000",
        api_key: str = None,  # Tambahkan parameter untuk API key
        **kwargs
    ):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.airbyte_url = airbyte_url
        self.api_key = api_key

    def execute(self, context):
        sync_endpoint = f"{self.airbyte_url}/api/v1/connections/sync"
        
        # Tambahkan headers untuk autentikasi
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        self.log.info(f"Memulai sinkronisasi untuk connection_id: {self.connection_id}")
        
        response = requests.post(
            sync_endpoint,
            headers=headers,
            json={"connectionId": self.connection_id}
        )
        
        if response.status_code != 200:
            raise Exception(f"Sinkronisasi gagal: {response.text}")
            
        return response.json()
    
class WaitForAirbyteSyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, airbyte_url, connection_ids, api_key, polling_interval=30, timeout=3600, *args, **kwargs):
        super(WaitForAirbyteSyncOperator, self).__init__(*args, **kwargs)
        self.airbyte_url = airbyte_url
        self.connection_ids = connection_ids
        self.api_key = api_key
        self.polling_interval = polling_interval  # Interval between polling in seconds
        self.timeout = timeout  # Maximum timeout in seconds

    def execute(self, context):
        """
        Waits until all connection_ids have their latest job with a status of 'succeeded'.
        If not, raises an exception.
        """
        start_time = time.time()
        pending_connections = set(self.connection_ids)

        while pending_connections:
            self.log.info(f"Checking status for connection_ids: {pending_connections}")

            for connection_id in list(pending_connections):
                response = requests.post(
                    f"{self.airbyte_url}/api/v1/jobs/list",
                    json={"configId": connection_id, "configTypes": ["sync"]},
                    headers={"Authorization": f"Bearer {self.api_key}"}
                )

                if response.status_code == 200:
                    job_data = response.json()
                    jobs = job_data.get("jobs", [])

                    if not jobs:
                        self.log.warning(f"No jobs found for connection_id: {connection_id}")
                        continue

                    # Get the latest job
                    latest_job = jobs[0]
                    job_status = latest_job['job']['status'].lower()

                    if job_status == "succeeded":
                        self.log.info(f"The latest job for connection_id {connection_id} succeeded.")
                        pending_connections.remove(connection_id)
                    elif job_status in ["failed", "cancelled"]:
                        raise ValueError(f"The latest job for connection_id {connection_id} failed with status: {job_status}")
                    else:
                        self.log.info(f"The latest job for connection_id {connection_id} is still running, status: {job_status}")
                else:
                    self.log.error(f"Failed to fetch the job list for connection_id {connection_id}, "
                                   f"status code: {response.status_code}")

            # Check for timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > self.timeout:
                raise TimeoutError(f"Timeout while waiting for syncs for connection_ids: {pending_connections}")

            # Wait before the next polling
            if pending_connections:
                self.log.info(f"Waiting {self.polling_interval} seconds before rechecking...")
                time.sleep(self.polling_interval)

        self.log.info("All Airbyte syncs succeeded.")
