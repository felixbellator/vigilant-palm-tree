#!/usr/bin/env python3
import json
import csv
import argparse
from typing import Any, List, Dict

def extract_apps(data: Any) -> List[Dict[str, str]]:
    """
    Extract `app_name` and `host` values from a JSON structure.
    Supports:
      - Top-level list
      - Objects with "data", "items", "applications" arrays
    """
    rows = []

    # Find the list of objects
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # try common container keys
        for k in ["data", "items", "applications", "private_apps", "result"]:
            if k in data and isinstance(data[k], list):
                items = data[k]
                break
        else:
            # fallback: treat values as possible lists
            items = []
            for v in data.values():
                if isinstance(v, list):
                    items.extend(v)
    else:
        items = []

    # Extract fields
    for obj in items:
        if not isinstance(obj, dict):
            continue
        app_name = obj.get("app_name", "")
        host = obj.get("host", "")

        # Some payloads may have host nested under "destinations" or "resources"
        if not host and "destinations" in obj:
            dests = obj["destinations"]
            if isinstance(dests, list) and dests:
                host = dests[0].get("host", "") or dests[0].get("fqdn", "")
        rows.append({
            "app_name": str(app_name),
            "host": str(host),
        })
    return rows

def main():
    parser = argparse.ArgumentParser(description="Parse JSON for app_name and host fields, export to CSV.")
    parser.add_argument("--in-json", required=True, help="Path to input JSON file.")
    parser.add_argument("--out-csv", default="apps_and_hosts.csv", help="Output CSV path.")
    args = parser.parse_args()

    with open(args.in_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = extract_apps(data)
    print(f"[INFO] Extracted {len(rows)} rows from {args.in_json}")

    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["app_name", "host"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[INFO] Wrote CSV to {args.out_csv}")

if __name__ == "__main__":
    main()
