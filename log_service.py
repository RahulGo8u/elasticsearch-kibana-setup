import logging
import re
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import quote

from kibana_client import search_logs
from config import PROD_ACCOUNT, DEFAULT_SIZE, KIBANA_APP_BASE_URL

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


def _end_time_epoch_ms(end_time: str) -> int:
    """Returns epoch milliseconds for logPosition. Uses end_time if absolute, else now."""
    end_dt = _parse_absolute_time(end_time)
    if end_dt is not None:
        return int(end_dt.timestamp() * 1000)
    return int(datetime.now().timestamp() * 1000)


def _build_kibana_logs_url(identifier: str, start_time: str, end_time: str) -> str:
    """Builds Kibana Logs stream URL with same filters and time range for troubleshooting."""
    kql = f'cloud.account.name : "{PROD_ACCOUNT}" and message : *{identifier}*'
    log_filter = f"(expression:'{quote(kql, safe='*')}',kind:kuery)"
    time_range = f"(time:(from:'{start_time}',to:'{end_time}'))"
    position_time_ms = _end_time_epoch_ms(end_time)
    log_position = f"(position:(tiebreaker:0,time:{position_time_ms}),streamLive:!f)"
    params = [
        f"_g={time_range}",
        "flyoutOptions=(flyoutId:!n,flyoutVisibility:hidden,surroundingLogsId:!n)",
        f"logFilter={log_filter}",
        "logMinimap=(intervalSize:2592000000)",
        f"logPosition={log_position}",
    ]
    return f"{KIBANA_APP_BASE_URL}/app/infra#/logs/stream?" + "&".join(params)


def _extract_stack_trace(message: str) -> Optional[str]:
    """Extracts the first stack trace found in a log message.

    Handles two formats:
    1. Enterprise Library logs: content after 'StackTrace Information Details:' header.
    2. Direct exception logs: consecutive lines starting with '   at '.
    """
    if not message:
        return None

    # Format 1: Enterprise Library embedded stack trace section
    match = re.search(
        r"StackTrace Information Details:\s*=+\s*(.*?)(?=Exception Information Details:|StackTrace Information Details:|$)",
        message,
        re.DOTALL,
    )
    if match:
        trace = match.group(1).strip()
        if trace:
            return trace

    # Format 2: direct 'at ...' lines (e.g. GeoHub-style logs)
    at_lines = re.findall(r"^\s{1,}at .+", message, re.MULTILINE)
    if at_lines:
        return "\n".join(line.strip() for line in at_lines)

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
               search_fn=search_logs) -> Dict:

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

    kibana_url = _build_kibana_logs_url(identifier.strip(), start_time, end_time)

    try:
        data = search_fn(query_body)
    except Exception as e:
        print(f"Error while fetching logs: {e}")
        return {"logs": [], "kibanaUrl": kibana_url}

    results = []
    for hit in data.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})

        message = source.get("message")
        results.append({
            "identifier": identifier,
            "timestamp": source.get("@timestamp"),
            "message": message,
            "stackTrace": _extract_stack_trace(message),
            "autoscalingGroupName": source.get("cloud.account", {}).get("autoscalingGroupName"),
            "accountName": source.get("cloud.account", {}).get("name"),
            "hostname": source.get("host", {}).get("name"),
            "_index": hit.get("_index"),
        })

    return {"logs": results, "kibanaUrl": kibana_url}