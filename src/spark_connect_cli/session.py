"""Spark Connect session, read-only guard, and output formatting.

Connecting to Spark Connect needs NO Kerberos and NO JVM on the client side —
the server runs under its own keytab and does the auth. The endpoint is a plain
gRPC address, e.g. sc://spark-connect:15002.
"""
from __future__ import annotations

import json
import os
import re
import socket
import sys

DEFAULT_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
DEFAULT_MAX_ROWS = int(os.environ.get("SCQ_MAX_ROWS", "10000"))
# How long to wait for the endpoint to accept a TCP connection before giving up
# with EXIT_CONN_ERR. Spark Connect's gRPC client otherwise retries a dead
# endpoint for a long time, trapping the caller — the opposite of what an
# agent-friendly CLI promises.
DEFAULT_CONNECT_TIMEOUT = float(os.environ.get("SCQ_CONNECT_TIMEOUT", "10"))

# Exit codes — stable contract so an agent can branch on them.
EXIT_OK = 0
EXIT_QUERY_ERR = 1
EXIT_CONN_ERR = 2
EXIT_BLOCKED = 3      # read-only guard tripped
EXIT_JOB_ERR = 4      # job-control error (no such job, etc.)

READ_ONLY_LEADERS = ("select", "show", "describe", "desc", "explain", "with")
# A CTE can prefix a write — `WITH x AS (...) INSERT INTO ...` — so a `WITH`
# leader alone does not prove the statement only reads. If any of these verbs
# appear (as whole words) we treat a WITH-led statement as a write. This can
# over-block an exotic read whose CTE mentions one of these in a string/column
# (rare); the caller can still pass --allow-ddl. Bias is toward safety.
_WRITE_KW = re.compile(
    r"\b(insert|update|delete|merge|overwrite|create|drop|alter|truncate|"
    r"replace|grant|revoke|call)\b", re.IGNORECASE)
# Leading SQL comments (line `--` or block `/* */`) must be stripped before we
# can see the real leader, otherwise a commented read is wrongly blocked.
_LEAD_COMMENT = re.compile(r"^\s*(?:--[^\n]*\n|/\*.*?\*/)", re.DOTALL)


def err(msg: object, code: int) -> None:
    """Emit a single JSON error object on stderr and exit."""
    print(json.dumps({"error": str(msg), "code": code}), file=sys.stderr)
    sys.exit(code)


def _strip_leading_comments(sql: str) -> str:
    prev = None
    s = sql.lstrip()
    while s != prev:
        prev = s
        s = _LEAD_COMMENT.sub("", s, count=1).lstrip()
    return s


def is_read_only(sql: str) -> bool:
    """True if the statement only reads (SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)."""
    s = _strip_leading_comments(sql).lstrip("(").lstrip()
    leader = s.split(None, 1)
    if not leader or leader[0].lower() not in READ_ONLY_LEADERS:
        return False
    # Only the WITH form can hide a write behind a read leader; SELECT/SHOW/etc.
    # cannot mutate regardless of body.
    if leader[0].lower() == "with" and _WRITE_KW.search(s):
        return False
    return True


def _preflight(remote: str, timeout: float) -> None:
    """Fail fast with EXIT_CONN_ERR if the endpoint isn't accepting TCP.

    Spark Connect's gRPC client retries a dead endpoint for a long time, which
    would trap the caller — so we probe the socket first with a short timeout.
    """
    rest = remote.split("://", 1)[-1]
    hostport = rest.split("/", 1)[0].split(";", 1)[0]
    if ":" in hostport:
        host, _, port_s = hostport.rpartition(":")
        try:
            port = int(port_s)
        except ValueError:
            host, port = hostport, 15002
    else:
        host, port = hostport, 15002
    host = host or "localhost"
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return
    except OSError as e:
        err(f"could not reach Spark Connect at {remote}: {e}", EXIT_CONN_ERR)


def get_spark(remote: str):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError:
        err("pyspark is not installed; `pip install 'pyspark[connect]>=4.1,<4.2'`", EXIT_CONN_ERR)
    _preflight(remote, DEFAULT_CONNECT_TIMEOUT)
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
