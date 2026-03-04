import pytest
from log_service import fetch_exception_logs


# -------------------------------
# Mock Success Response (Logs Found)
# -------------------------------
def mock_search_logs_success(query):
    return {
        "hits": {
            "hits": [
                {
                    "_index": "test-index",
                    "_source": {
                        "message": "Critical exception occurred in service",
                        "@timestamp": "2026-01-01T00:00:00Z",
                        "host": {"name": "test-host"},
                        "cloud": {
                            "account": {
                                "autoscalingGroupName": "test-asg"
                            }
                        },
                        "log": {
                            "file": {
                                "path": "/var/log/test.log"
                            }
                        }
                    }
                }
            ]
        }
    }


# -------------------------------
# Mock Empty Response (No Logs)
# -------------------------------
def mock_search_logs_empty(query):
    return {
        "hits": {
            "hits": []
        }
    }


# -------------------------------
# Mock Failure Response (ES Down)
# -------------------------------
def mock_search_logs_failure(query):
    raise Exception("Elasticsearch connection failed")


# ===============================
# TEST CASES
# ===============================

def test_logs_found():
    logs = fetch_exception_logs(
        identifier="service",
        search_fn=mock_search_logs_success
    )

    assert isinstance(logs, list)
    assert len(logs) == 1
    assert "exception" in logs[0]["message"].lower()
    assert logs[0]["hostname"] == "test-host"
    assert logs[0]["autoscalingGroupName"] == "test-asg"


def test_no_logs_found():
    logs = fetch_exception_logs(
        identifier="randomstring",
        search_fn=mock_search_logs_empty
    )

    assert isinstance(logs, list)
    assert len(logs) == 0


def test_invalid_identifier():
    with pytest.raises(ValueError):
        fetch_exception_logs(
            identifier="",
            search_fn=mock_search_logs_success
        )


def test_elasticsearch_failure():
    logs = fetch_exception_logs(
        identifier="service",
        search_fn=mock_search_logs_failure
    )

    assert logs == []