#!/usr/bin/env python3
import argparse, json, sys
from typing import Any, Dict, List, Set
import requests, pandas as pd

def fetch(url: str, headers: Dict[str, str], verify_tls: bool = True, timeout: int = 30) -> Any:
    r = requests.get(url, headers=headers, verify=verify_tls, timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print(f"[ERROR] Netskope API {r.status_code}: {r.text}", file=sys.stderr)
        raise
    return r.json()

def extract_items(payload: Any) -> List[Dict[str, Any]]:
    """
    Find the list of app objects in common response shapes.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "result", "private_apps", "applications"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        # fallback: first list of dicts
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []

def harvest_hosts(obj: Any) -> Set[str]:
    """
    Collect destination hostnames/domains from typical fields:
      - direct: fqdn, hostname, host, domain, destination, destination_fqdn
      - containers: destinations, resources (and their children)
    """
    out: Set[str] = set()
    def walk(v: Any):
        if v is None:
            return
        if isinstance(v, str):
            s = v.strip()
            if s: out.add(s)
            return
        if isinstance(v, list):
            for x in v: walk(x)
            return
        if isinstance(v, dict):
            # leaf-ish keys
            for k in ("fqdn","hostname","host","domain","destination","destination_fqdn"):
                if k in v: walk(v[k])
            # container-ish keys
            for k in ("destinations","resources","domains"):
                if k in v: walk(v[k])
            # scan remaining children
            for vv in v.values():
                if isinstance(vv, (dict, list, str)): walk(vv)
    walk(obj)
    return out

def row_from_app(app: Dict[str, Any]) -> Dict[str, str]:
    # App name: prioritize 'app_name'
    name = ""
    for k in ("app_name", "name", "application_name", "display_name", "label"):
        if isinstance(app.get(k), str) and app[k].strip():
            name = " ".join(app[k].split()).strip()
            break

    # App ID if present
    app_id = ""
    for k in ("id","app_id","uuid","guid"):
        if k in app and app[k] is not None:
            app_id = str(app[k]); break

    # Hosts
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

    items = extract_items(payload)
    if not items:
        print("[WARN] No app list found in response. Adjust extract_items() for your endpoint.", file=sys.stderr)

    rows = [row_from_app(app) for app in items]
    df = pd.DataFrame(rows, columns=["Application Name","Destination Hostnames","App ID"])
    df = df.sort_values(by="Application Name", key=lambda s: s.str.lower(), kind="mergesort")
    df.to_csv(args.out_csv, index=False)
    print(f"Exported {len(df)} apps to {args.out_csv}")

if __name__ == "__main__":
    main()
                nxt = None
                break
        # Stop if no next cursor
        if not nxt:
            break
        cursor = str(nxt)

    return pages


def extract_apps(data_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Try to locate the array of private apps across common shapes:
      - top-level list
      - {"data":[...]}
      - {"items":[...]}
      - {"result":[...]}
      - or nested under keys like "private_apps", "applications"
    """
    candidates = []
    for page in data_pages:
        arr = None
        if isinstance(page, list):
            arr = page
        elif isinstance(page, dict):
            for key in ["data", "items", "result", "private_apps", "applications"]:
                if key in page and isinstance(page[key], list):
                    arr = page[key]
                    break
            if arr is None:
                # Fallback: scan for first list of dicts
                for v in page.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        arr = v
                        break
        if arr:
            candidates.extend(arr)
    return candidates


def to_host_set(obj: Any) -> Set[str]:
    """
    Harvest destination hostnames/domains/fqdns from various shapes.
    Looks for keys commonly seen in NPA payloads.
    """
    out: Set[str] = set()

    def harvest(v: Any):
        if v is None:
            return
        if isinstance(v, str):
            s = v.strip()
            if s:
                out.add(s)
        elif isinstance(v, list):
            for item in v:
                harvest(item)
        elif isinstance(v, dict):
            # common leaf keys
            for k in ["fqdn", "hostname", "host", "domain", "destination", "destination_fqdn"]:
                if k in v:
                    harvest(v[k])
            # common containers
            for k in ["destinations", "resources", "connectors", "apps", "domains"]:
                if k in v:
                    harvest(v[k])
            # scan everything else
            for vv in v.values():
                if isinstance(vv, (dict, list, str)):
                    harvest(vv)

    harvest(obj)
    return out


def normalize(s: str) -> str:
    return " ".join(s.split()).strip()


def row_from_app(app: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a single CSV row from an app object:
      Application Name, Destination Hostnames, App ID (if any)
    """
    # try name-ish keys
    name = None
    for k in ["name", "app_name", "application", "application_name", "display_name", "label"]:
        if k in app and isinstance(app[k], str) and app[k].strip():
            name = normalize(app[k])
            break

    app_id = None
    for k in ["id", "app_id", "uuid", "guid"]:
        if k in app and isinstance(app[k], (str, int)):
            app_id = str(app[k])
            break

    hosts = to_host_set(app)
    hosts_list = sorted({normalize(h) for h in hosts if h})

    return {
        "Application Name": name or "",
        "Destination Hostnames": ", ".join(hosts_list),
        "App ID": app_id or "",
    }


def export_to_csv(rows: List[Dict[str, Any]], out_csv: str):
    df = pd.DataFrame(rows, columns=["Application Name", "Destination Hostnames", "App ID"])
    # sort by app name, case-insensitive
    df = df.sort_values(by="Application Name", key=lambda s: s.str.lower(), kind="mergesort")
    df.to_csv(out_csv, index=False)
    return out_csv


def main():
    ap = argparse.ArgumentParser(description="Export Netskope NPA Private Applications to CSV.")
    ap.add_argument("--url", required=True, help="Full Netskope API URL to list NPA private apps.")
    ap.add_argument("--token", required=True, help="API token value. If using Bearer, include the 'Bearer ' prefix.")
    ap.add_argument("--token-header", default="Netskope-Api-Token",
                    help='Header key for token (default: Netskope-Api-Token). Use "Authorization" for Bearer.')
    ap.add_argument("--out-csv", default="netskope_npa_private_apps.csv", help="CSV output path.")
    ap.add_argument("--raw-json", default=None, help="Optional path to save the raw JSON from all pages.")
    ap.add_argument("--insecure", action="store_true", help="Disable TLS verification (not recommended).")
    # Optional pagination knobs (leave defaults if your endpoint returns everything at once)
    ap.add_argument("--cursor-param", default=None, help="Name of cursor/page query parameter (e.g., 'cursor').")
    ap.add_argument("--next-cursor-path", default=None,
                    help="JSON path to the next cursor, e.g., 'meta.next' or 'pagination.next'.")
    ap.add_argument("--per-page", default=None, help="If API supports larger pages, pass like 'limit=1000'.")
    args = ap.parse_args()

    headers = {args.token_header: args.token}

    next_cursor_path_list = None
    if args.next_cursor_path:
        next_cursor_path_list = [p for p in args.next_cursor_path.split(".") if p]

    per_page_tuple = None
    if args.per_page and "=" in args.per_page:
        k, v = args.per_page.split("=", 1)
        per_page_tuple = (k, v)

    pages = fetch_all_pages(
        url=args.url,
        headers=headers,
        verify_tls=not args.insecure,
        pagination_param=args.cursor_param,
        next_cursor_path=next_cursor_path_list,
        per_page_param=per_page_tuple,
    )

    # Optionally write raw JSON (either a single object or an array of pages)
    if args.raw_json:
        # If single page, keep it simple; else store list of pages
        payload = pages[0] if len(pages) == 1 else pages
        with open(args.raw_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    apps = extract_apps(pages)
    if not apps:
        print("[WARN] No app objects found in response. "
              "If your endpoint uses a different container key, you may need to adjust `extract_apps()`.",
              file=sys.stderr)

    rows = [row_from_app(a) for a in apps]
    export_to_csv(rows, args.out_csv)
    print(f"Exported {len(rows)} apps to {args.out_csv}")
    if args.raw_json:
        print(f"Raw JSON saved to {args.raw_json}")


if __name__ == "__main__":
    main()
