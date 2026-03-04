from typing import List, Dict
from kibana_client import search_logs
from config import PROD_ACCOUNT, DEFAULT_SIZE


def fetch_exception_logs(identifier: str,
                         start_time: str = "now-90d",
                         end_time: str = "now",
                         search_fn=search_logs) -> List[Dict]:

    # ✅ Input validation
    if not identifier or not identifier.strip():
        raise ValueError("Identifier cannot be empty")

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