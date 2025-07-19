import logging
import os
import traceback
import azure.functions as func
import json
import requests

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Function triggered.')

    try:
        # Parse JSON body
        req_body = req.get_json()

        # STEP 1: Handle Event Grid webhook validation handshake
        if "validationCode" in req_body:
            logging.info("Validation handshake received.")
            return func.HttpResponse(
                json.dumps({"validationResponse": req_body["validationCode"]}),
                status_code=200,
                mimetype="application/json"
            )

        # STEP 2: Proceed with Databricks job trigger
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

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        logging.error(f"Error occurred: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(
            f"Function failed: {str(e)}",
            status_code=500
        )