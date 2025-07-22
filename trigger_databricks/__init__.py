import logging
import os
import azure.functions as func
import json
import requests
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Function triggered.')

    try:
        # Parse request body
        req_body = req.get_json()
        logging.info(f"Request body: {req_body}")

        # STEP 1: Event Grid validation handshake
        if "validationCode" in req_body:
            logging.info("Validation handshake received.")
            return func.HttpResponse(
                json.dumps({"validationResponse": req_body["validationCode"]}),
                status_code=200,
                mimetype="application/json"
            )

        # STEP 2: Trigger Databricks job
        databricks_host = os.environ.get("DATABRICKS_HOST")
        databricks_token = os.environ.get("DATABRICKS_TOKEN")
        job_id = os.environ.get("DATABRICKS_JOB_ID")

        if not databricks_host or not databricks_token or not job_id:
            raise ValueError("One or more environment variables are missing.")

        logging.info(f"Triggering Databricks job ID: {job_id} at {databricks_host}")

        response = requests.post(
            f"{databricks_host}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {databricks_token}"},
            json={"job_id": int(job_id)}
        )

        logging.info(f"Databricks response: {response.status_code} - {response.text}")

        if response.status_code >= 400:
            return func.HttpResponse(
                f"Databricks job trigger failed: {response.text}",
                status_code=response.status_code
            )

        return func.HttpResponse(
            f"Databricks job triggered successfully: {response.status_code}",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(
            f"Function failed: {str(e)}",
            status_code=500
        )