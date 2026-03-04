import logging
from datetime import datetime
from typing import List, Dict, Optional
from kibana_client import search_logs
from config import PROD_ACCOUNT, DEFAULT_SIZE

_LOG_FILE = "search.log"
_logger = logging.getLogger(__name__)


def _ensure_file_logging() -> None:
    if not _logger.handlers:
        _logger.setLevel(logging.INFO)
        handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        _logger.addHandler(handler)


def _parse_absolute_time(time_str: str) -> Optional[datetime]:
    """Returns a datetime if time_str is an ISO 8601 absolute timestamp, else None."""
    try:
        return datetime.fromisoformat(time_str.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _validate_time_range(start_time: str, end_time: str) -> None:
    """Validates end_time > start_time when both are absolute ISO 8601 datetimes.
    Skips validation if either value is a relative expression (e.g. 'now-90d')."""
    start_dt = _parse_absolute_time(start_time)
    end_dt = _parse_absolute_time(end_time)
    if start_dt is not None and end_dt is not None:
        if end_dt <= start_dt:
            raise ValueError(
                f"end_time must be after start_time. "
                f"Got start_time='{start_time}', end_time='{end_time}'"
            )


def fetch_logs(identifier: str,
               start_time: str = "now-90d",
               end_time: str = "now",
               search_fn=search_logs) -> List[Dict]:

    if not identifier or not identifier.strip():
        raise ValueError("Identifier cannot be empty")

    _validate_time_range(start_time, end_time)
    _ensure_file_logging()
    _logger.info("Searching for identifier: %s", identifier.strip())

    query_body = {
        "size": DEFAULT_SIZE,
        "sort": [
            {
                "@timestamp": {
                    "order": "desc",
                    "unmapped_type": "date",
                    "missing": "_last"
                }
            }
        ],
        "_source": [
            "@timestamp",
            "message",
            "event.original",
            "log.original",
            "cloud.account",
            "cloud.account.name",
            "host.name"
        ],
        "query": {
            "bool": {
                "filter": [
                    { "match_phrase": { "cloud.account.name": PROD_ACCOUNT } },
                    {
                        "query_string": {
                            "fields": ["message", "event.original", "log.original"],
                            "query": f"*{identifier}*"
                        }
                    },
                    { "range": { "@timestamp": { "gte": start_time, "lte": end_time } } }
                ]
            }
        }
    }

    try:
        data = search_fn(query_body)
    except Exception as e:
        print(f"Error while fetching logs: {e}")
        return []

    results = []

    for hit in data.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})

        results.append({
            "identifier": identifier,
            "timestamp": source.get("@timestamp"),
            "message": source.get("message"),
            "autoscalingGroupName": source.get("cloud.account", {}).get("autoscalingGroupName"),
            "accountName": source.get("cloud.account", {}).get("name"),
            "hostname": source.get("host", {}).get("name"),
            "_index": hit.get("_index"),
        })

    return results