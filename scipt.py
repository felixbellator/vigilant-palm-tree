#!/usr/bin/env python3
import argparse, json, sys
from typing import Any, Dict, List, Set
import requests, pandas as pd

def fetch(url: str, headers: Dict[str, str], verify_tls: bool = True, timeout: int = 30) -> Any:
    print(f"[INFO] Calling Netskope API: {url}")
    r = requests.get(url, headers=headers, verify=verify_tls, timeout=timeout)
    print(f"[INFO] HTTP status: {r.status_code}")
    if r.ok:
        print("[INFO] API call successful.")
    else:
        print(f"[ERROR] API call failed: {r.text[:500]}")
        r.raise_for_status()
    return r.json()

def extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        print(f"[DEBUG] Top-level JSON is a list with {len(payload)} items.")
        return payload
    if isinstance(payload, dict):
        print(f"[DEBUG] Top-level JSON is a dict with keys: {list(payload.keys())}")
        for key in ("data", "items", "result", "private_apps", "applications"):
            v = payload.get(key)
            if isinstance(v, list):
                print(f"[INFO] Found list of apps under key '{key}' with {len(v)} items.")
                return v
        # fallback: first list of dicts
        for k, v in payload.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                print(f"[INFO] Found list of dicts under key '{k}' with {len(v)} items.")
                return v
    print("[WARN] Could not locate app list in JSON payload.")
    return []

def harvest_hosts(obj: Any) -> Set[str]:
    out: Set[str] = set()
    def walk(v: Any):
        if v is None: return
        if isinstance(v, str):
            s = v.strip()
            if s: out.add(s); return
        if isinstance(v, list):
            for x in v: walk(x); return
        if isinstance(v, dict):
            for k in ("fqdn","hostname","host","domain","destination","destination_fqdn"):
                if k in v: walk(v[k])
            for k in ("destinations","resources","domains"):
                if k in v: walk(v[k])
            for vv in v.values():
                if isinstance(vv, (dict, list, str)): walk(vv)
    walk(obj)
    return out

def row_from_app(app: Dict[str, Any]) -> Dict[str, str]:
    name = ""
    for k in ("app_name", "name", "application_name", "display_name", "label"):
        if isinstance(app.get(k), str) and app[k].strip():
            name = " ".join(app[k].split()).strip(); break
    app_id = ""
    for k in ("id","app_id","uuid","guid"):
        if k in app and app[k] is not None:
            app_id = str(app[k]); break
    hosts = sorted({ " ".join(h.split()).strip() for h in harvest_hosts(app) if h })
    return {
        "Application Name": name,
        "Destination Hostnames": ", ".join(hosts),
        "App ID": app_id
    }

def main():
    ap = argparse.ArgumentParser(description="Export Netskope NPA Private Applications to CSV (uses app_name).")
    ap.add_argument("--url", required=True, help="Full Netskope API URL to list NPA private apps.")
    ap.add_argument("--token", required=True, help="Token value. If using Bearer, include 'Bearer ...'.")
    ap.add_argument("--token-header", default="Netskope-Api-Token",
                   help='Header name for token (default Netskope-Api-Token; use "Authorization" for Bearer).')
    ap.add_argument("--out-csv", default="netskope_npa_private_apps.csv", help="CSV output path.")
    ap.add_argument("--raw-json", default=None, help="Optional: save raw JSON to this path.")
    ap.add_argument("--insecure", action="store_true", help="Disable TLS verification.")
    args = ap.parse_args()

    headers = {args.token_header: args.token}
    payload = fetch(args.url, headers, verify_tls=not args.insecure)

    if args.raw_json:
        with open(args.raw_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        print(f"[INFO] Raw JSON written to {args.raw_json}")

    items = extract_items(payload)
    print(f"[INFO] Extracted {len(items)} app objects from payload.")

    rows = [row_from_app(app) for app in items]
    df = pd.DataFrame(rows, columns=["Application Name","Destination Hostnames","App ID"])
    df = df.sort_values(by="Application Name", key=lambda s: s.str.lower(), kind="mergesort")
    df.to_csv(args.out_csv, index=False)
    print(f"[INFO] Exported {len(df)} rows to {args.out_csv}")

if __name__ == "__main__":
    main()
