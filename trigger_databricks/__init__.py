import logging
import requests # type: ignore
import os
import azure.functions as func # type: ignore

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python function triggered.')

    databricks_host = os.environ.get("DATABRICKS_HOST")
    databricks_token = os.environ.get("DATABRICKS_TOKEN")
    job_id = os.environ.get("DATABRICKS_JOB_ID")

    response = requests.post(
        f"{databricks_host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {databricks_token}"},
        json={"job_id": job_id}
    )

    return func.HttpResponse(
        f"Triggered Databricks job: {response.status_code}",
        status_code=200
    )
