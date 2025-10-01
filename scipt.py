#!/usr/bin/env python3
import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
import pandas as pd


def fetch_all_pages(
    url: str,
    headers: Dict[str, str],
    verify_tls: bool = True,
    timeout: int = 30,
    pagination_param: Optional[str] = None,
    next_cursor_path: Optional[List[str]] = None,
    per_page_param: Optional[Tuple[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch JSON from Netskope endpoint; supports cursor/offset style pagination if configured.

    Args:
      url: full API URL to list private apps
      headers: auth headers
      pagination_param: query parameter name for cursor/offset (e.g., "cursor", "page")
      next_cursor_path: JSON path to read the "next" cursor from response (e.g., ["meta","next"])
      per_page_param: (key, value) to request bigger pages if API supports (e.g., ("limit","1000"))

    Returns:
      List of response objects (each page as dict). If pagination is not configured, returns one item list.
    """
    pages: List[Dict[str, Any]] = []
    session = requests.Session()

    params: Dict[str, str] = {}
    if per_page_param:
        k, v = per_page_param
        params[k] = v

    cursor: Optional[str] = None
    while True:
        req_params = dict(params)
        if pagination_param and cursor:
            req_params[pagination_param] = cursor

        resp = session.get(url, headers=headers, params=req_params, timeout=timeout, verify=verify_tls)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise SystemExit(f"[ERROR] Netskope API error {resp.status_code}: {resp.text}") from e

        data = resp.json()
        pages.append(data)

        if not pagination_param or not next_cursor_path:
            break  # single page
        # Walk JSON to find next cursor
        nxt = data
        for key in next_cursor_path:
            if isinstance(nxt, dict) and key in nxt:
                nxt = nxt[key]
            else:
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
