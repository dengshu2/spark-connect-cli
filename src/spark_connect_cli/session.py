"""Spark Connect session, read-only guard, and output formatting.

Connecting to Spark Connect needs NO Kerberos and NO JVM on the client side —
the server runs under its own keytab and does the auth. The endpoint is a plain
gRPC address, e.g. sc://spark-connect:15002.
"""
from __future__ import annotations

import json
import os
import sys

DEFAULT_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
DEFAULT_MAX_ROWS = int(os.environ.get("SCQ_MAX_ROWS", "10000"))

# Exit codes — stable contract so an agent can branch on them.
EXIT_OK = 0
EXIT_QUERY_ERR = 1
EXIT_CONN_ERR = 2
EXIT_BLOCKED = 3      # read-only guard tripped
EXIT_JOB_ERR = 4      # job-control error (no such job, etc.)

READ_ONLY_LEADERS = ("select", "show", "describe", "desc", "explain", "with")


def err(msg: object, code: int) -> None:
    """Emit a single JSON error object on stderr and exit."""
    print(json.dumps({"error": str(msg), "code": code}), file=sys.stderr)
    sys.exit(code)


def is_read_only(sql: str) -> bool:
    """True if the statement only reads (SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)."""
    leader = sql.strip().lstrip("(").split(None, 1)
    return bool(leader) and leader[0].lower() in READ_ONLY_LEADERS


def get_spark(remote: str):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError:
        err("pyspark is not installed; `pip install 'pyspark[connect]>=3.5,<4'`", EXIT_CONN_ERR)
    try:
        return SparkSession.builder.remote(remote).getOrCreate()
    except Exception as e:  # noqa: BLE001
        err(f"could not connect to Spark Connect at {remote}: {e}", EXIT_CONN_ERR)


def emit_rows(rows, columns, fmt: str) -> None:
    """Render a result set. JSON-first (JSONEachRow) by default."""
    if fmt == "jsoneachrow":
        for r in rows:
            print(json.dumps(dict(zip(columns, r)), default=str, ensure_ascii=False))
    elif fmt == "json":
        print(json.dumps(
            {"meta": list(columns), "data": [list(r) for r in rows], "rows": len(rows)},
            default=str, ensure_ascii=False))
    elif fmt in ("csv", "tsv"):
        sep = "," if fmt == "csv" else "\t"
        print(sep.join(columns))
        for r in rows:
            print(sep.join("" if v is None else str(v) for v in r))
    elif fmt == "table":
        widths = [len(c) for c in columns]
        srows = [["" if v is None else str(v) for v in r] for r in rows]
        for r in srows:
            for i, v in enumerate(r):
                widths[i] = max(widths[i], len(v))
        print(" | ".join(c.ljust(widths[i]) for i, c in enumerate(columns)))
        print("-+-".join("-" * w for w in widths))
        for r in srows:
            print(" | ".join(v.ljust(widths[i]) for i, v in enumerate(r)))
