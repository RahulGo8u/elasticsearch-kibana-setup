# main.py

from log_service import fetch_logs


if __name__ == "__main__":
    logs = fetch_logs(
        identifier="exception",
        start_time="now-90d",
        end_time="now-1m"
    )

    print(f"\nTotal Logs Found: {len(logs)}\n")

    for i, log in enumerate(logs, start=1):
        print("=" * 80)
        print(f"Log #{i}")
        print("-" * 80)

        print(f"Identifier           : {log.get('identifier')}")
        print(f"Timestamp            : {log.get('timestamp')}")
        print(f"Index                : {log.get('_index')}")
        print(f"Autoscaling Group    : {log.get('autoscalingGroupName')}")
        print(f"Hostname             : {log.get('hostname')}")
        print(f"Message              :\n{log.get('message')}")

        print("=" * 80)
        print("\n")