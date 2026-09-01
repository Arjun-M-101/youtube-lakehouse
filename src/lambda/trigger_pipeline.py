"""
trigger_pipeline.py — AWS Lambda

Fires on an S3 ObjectCreated event in the Bronze prefix and starts a Step
Functions execution of the batch pipeline state machine. This Lambda does
almost nothing on purpose — its only job is "a file landed, wake the
orchestrator up." All the actual pipeline logic lives in Step Functions +
Glue, not here. Keeping Lambdas thin like this is itself a deliberate
design choice worth naming in an interview: it means retries, DQ gating,
and branching all live in one place (the state machine) instead of being
smeared across event handlers.

Environment variables (set via Terraform):
  STATE_MACHINE_ARN   ARN of the batch pipeline Step Functions state machine
"""

import json
import os
import uuid
from urllib.parse import unquote_plus

import boto3

sfn_client = boto3.client("stepfunctions", region_name=os.environ.get("AWS_REGION", "ap-south-1"))


def lambda_handler(event, context):
    state_machine_arn = os.environ["STATE_MACHINE_ARN"]

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        execution_id = uuid.uuid4()
        dq_report_key = f"dq-reports/bronze-to-silver/{execution_id}.json"

        execution_input = {
            "bucket": bucket,
            "triggeredKey": key,
            "dqReportKey": dq_report_key,
        }

        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"bronze-upload-{execution_id}",
            input=json.dumps(execution_input),
        )
        print(f"Started execution {response['executionArn']} for s3://{bucket}/{key}")

    return {"statusCode": 200, "body": "OK"}
