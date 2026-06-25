"""Read-only / discovery commands over Spark Connect."""
from __future__ import annotations

import json
import sys

from .session import (DEFAULT_MAX_ROWS, EXIT_BLOCKED, EXIT_QUERY_ERR, emit_rows,
                      err, get_spark, is_read_only)


def cmd_databases(args) -> None:
    spark = get_spark(args.remote)
    rows = spark.sql("SHOW DATABASES").collect()
    emit_rows([(r[0],) for r in rows], ["database"], args.format)


def cmd_tables(args) -> None:
    spark = get_spark(args.remote)
    db = args.database or "default"
    rows = spark.sql(f"SHOW TABLES IN `{db}`").collect()
    out = [(r["namespace"], r["tableName"]) for r in rows]
    if args.like:
        pat = args.like.replace("%", "").lower()
        out = [t for t in out if pat in t[1].lower()]
    emit_rows(out, ["database", "table"], args.format)


def cmd_describe(args) -> None:
    spark = get_spark(args.remote)
    rows = spark.sql(f"DESCRIBE TABLE {args.table}").collect()
    emit_rows([(r[0], r[1], r[2]) for r in rows],
              ["col_name", "data_type", "comment"], args.format)


def cmd_query(args) -> None:
    sql = args.sql
    if not args.allow_ddl and not is_read_only(sql):
        err("write/DDL blocked by read-only guard; pass --allow-ddl to override",
            EXIT_BLOCKED)
    spark = get_spark(args.remote)
    try:
        df = spark.sql(sql)
    except Exception as e:  # noqa: BLE001
        err(f"query failed: {e}", EXIT_QUERY_ERR)
    if not is_read_only(sql):
        print(json.dumps({"ok": True}))
        return
    max_rows = args.max_rows if args.max_rows is not None else DEFAULT_MAX_ROWS
    columns = df.columns
    limited = df.limit(max_rows + 1).collect() if max_rows > 0 else df.collect()
    truncated = max_rows > 0 and len(limited) > max_rows
    rows = limited[:max_rows] if truncated else limited
    emit_rows([tuple(r) for r in rows], columns, args.format)
    if truncated:
        print(json.dumps({"warning": f"result capped at {max_rows} rows; "
                                     "add LIMIT/filters or raise --max-rows"}),
              file=sys.stderr)
