# config.py

KIBANA_URL = "http://metrics-kibana.cmh.corp.evinternal.net/api/console/proxy"

HEADERS = {
    "Content-Type": "application/json",
    "kbn-xsrf": "true",
    "kbn-version": "7.5.0"
}

INDEX_PATTERN = "filebeat-*"
PROD_ACCOUNT = "evtech-reports-prod"
DEFAULT_SIZE = 50