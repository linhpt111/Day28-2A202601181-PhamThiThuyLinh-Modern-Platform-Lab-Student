"""Verify that a firing Prometheus alert reached the internal Alertmanager receiver."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--alert", default="Lab28ApiUnavailable")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--receiver-url", default="http://localhost:18080")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    with httpx.Client(timeout=10.0) as client:
        alerts_url = f"{args.prometheus_url.rstrip('/')}/api/v1/alerts"
        firing = client.get(alerts_url).json()["data"]["alerts"]
        received = client.get(f"{args.receiver_url.rstrip('/')}/alerts").json()["events"]

    report = {
        "alert": args.alert,
        "prometheus_currently_firing": any(
            item["labels"].get("alertname") == args.alert and item["state"] == "firing"
            for item in firing
        ),
        "receiver_firing_delivery": any(
            alert.get("labels", {}).get("alertname") == args.alert
            and alert.get("status") == "firing"
            for event in received
            for alert in event.get("alerts", [])
        ),
        "receiver_events": received,
    }
    # Alertmanager can resolve an alert before this command runs. A receiver
    # payload marked firing is still proof that Prometheus evaluated and routed
    # the alert through Alertmanager.
    if not report["receiver_firing_delivery"]:
        raise SystemExit(json.dumps(report, ensure_ascii=False, indent=2))
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
