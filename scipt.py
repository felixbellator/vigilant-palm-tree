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
                    if isinstance(v, list) and v an
