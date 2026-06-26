"""scq exec — read-only passthrough to the Spark REST API.

One generic accessor over the Spark monitoring REST API instead of a pile of
single-purpose diagnostics. The model composes the path and interprets the JSON,
so skew, slow stages, shuffle spill, executor GC/OOM, etc. are all the same
command with different readings.

Access path: the Spark UI redirects to the YARN ResourceManager web proxy, so we
discover the running Spark application via the RM REST and go through the proxy.
Pure-Python (urllib) — no curl, no manual app id. GET-only against /api/v1.
"""
from __future__ import annotations

import json
import os
import urllib.request

from .session import EXIT_CONN_ERR, err

DEFAULT_RM = os.environ.get("SCQ_YARN_RM", "http://namenode.hive-net:8088")


def _get_json(url: str, timeout: int = 15):
    with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 — fixed RM base
        return json.load(r)


def _discover_app(rm: str) -> str | None:
    apps = _get_json(f"{rm}/ws/v1/cluster/apps?states=RUNNING&applicationTypes=SPARK")
    lst = ((apps.get("apps") or {}).get("app")) or []
    for a in lst:  # prefer the Spark Connect Server
        if "connect" in (a.get("name") or "").lower():
            return a["id"]
    return lst[0]["id"] if lst else None


def cmd_exec(args) -> None:
    rm = (args.rm or DEFAULT_RM).rstrip("/")
    path = (args.path or "").lstrip("/")
    if ".." in path:
        err("path must not contain '..'", EXIT_CONN_ERR)
    try:
        app = _discover_app(rm)
        if not app:
            err("no RUNNING Spark application found via the YARN RM", EXIT_CONN_ERR)
        base = f"{rm}/proxy/{app}/api/v1/applications/{app}"
        data = _get_json(f"{base}/{path}" if path else base)
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        err(f"Spark REST request failed: {e}", EXIT_CONN_ERR)
    print(json.dumps(data, ensure_ascii=False,
                     indent=None if args.compact else 2, default=str))
