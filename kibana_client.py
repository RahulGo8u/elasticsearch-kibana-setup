# kibana_client.py

import requests
from config import KIBANA_URL, HEADERS, INDEX_PATTERN


def search_logs(query_body: dict) -> dict:
    """
    Generic search function for Kibana console proxy
    """

    params = {
        "path": f"{INDEX_PATTERN}/_search",
        "method": "POST"
    }

    response = requests.post(
        KIBANA_URL,
        params=params,
        headers=HEADERS,
        json=query_body
    )

    response.raise_for_status()

    return response.json()