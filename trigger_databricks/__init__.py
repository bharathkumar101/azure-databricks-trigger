import logging
import os
import azure.functions as func
import json
import urllib.request

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Function triggered.')

    try:
        events = req.get_json()

        # STEP 1: Handle Event Grid webhook validation
        for event in events:
            if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
                validation_code = event["data"]["validationCode"]
                logging.info("Validation handshake received.")
                return func.HttpResponse(
                    json.dumps({"validationResponse": validation_code}),
                    status_code=200,
                    mimetype="application/json"
                )
        
        print("test")
        # STEP 2: Trigger Databricks Job
        databricks_host = os.environ.get("DATABRICKS_HOST")
        databricks_token = os.environ.get("DATABRICKS_TOKEN")
        job_id = os.environ.get("DATABRICKS_JOB_ID")

        url = f"{databricks_host}/api/2.1/jobs/run-now"
        payload = json.dumps({"job_id": job_id}).encode("utf-8")

        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {databricks_token}"
            }
        )

        with urllib.request.urlopen(request) as response:
            response_body = response.read().decode("utf-8")
            logging.info(f"Databricks response: {response_body}")
            return func.HttpResponse(
                f"Triggered Databricks job: {response.status}",
                status_code=200
            )

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(
            f"Function failed: {str(e)}",
            status_code=500
        )
