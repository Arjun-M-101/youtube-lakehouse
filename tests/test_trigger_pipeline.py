import json, os, sys
from unittest.mock import MagicMock
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "lambda"))

S3_EVENT = {"Records":[{"s3":{"bucket":{"name":"bucket"},"object":{"key":"bronze%2Fyoutube%2FUSvideos.csv"}}}]}

def load_handler(monkeypatch):
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:ap-south-1:123:stateMachine:test")
    import importlib
    mod=importlib.import_module("trigger_pipeline")
    client=MagicMock()
    client.start_execution.return_value={"executionArn":"arn:test"}
    mod.sfn_client=client
    return mod, client


def test_starts_execution(monkeypatch):
    mod, client=load_handler(monkeypatch)
    result=mod.lambda_handler(S3_EVENT,None)
    assert result["statusCode"]==200
    kwargs=client.start_execution.call_args.kwargs
    assert kwargs["stateMachineArn"].endswith(":stateMachine:test")
    payload=json.loads(kwargs["input"])
    assert payload["bucket"]=="bucket"
    assert payload["triggeredKey"]=="bronze/youtube/USvideos.csv"


def test_multiple_records(monkeypatch):
    mod, client=load_handler(monkeypatch)
    event={"Records":[S3_EVENT["Records"][0],S3_EVENT["Records"][0]]}
    assert mod.lambda_handler(event,None)["statusCode"]==200
    assert client.start_execution.call_count==2


def test_empty_records(monkeypatch):
    mod, client=load_handler(monkeypatch)
    assert mod.lambda_handler({"Records":[]},None)["statusCode"]==200
    client.start_execution.assert_not_called()
