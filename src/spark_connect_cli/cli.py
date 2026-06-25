"""Argument parsing and dispatch for `scq` / `spark-connect-cli`."""
from __future__ import annotations

import json
import sys

from . import jobs, query
from .session import DEFAULT_REMOTE


def cmd_sync(args) -> None:
    argv = [args.source, "--to", args.to, "--remote", args.remote, "--mode", args.mode]
    if args.ch_jdbc:
        argv += ["--ch-jdbc", args.ch_jdbc]
    if args.target:
        argv += ["--target", args.target]
    if args.where:
        argv += ["--where", args.where]
    if args.limit:
        argv += ["--limit", str(args.limit)]
    job_id = jobs.submit("sync", argv,
                         {"source": args.source, "target": args.target or "", "to": args.to})
    print(json.dumps({
        "job_id": job_id, "state": "running",
        "message": f"sync of {args.source} -> {args.to} submitted; "
                   f"poll with `scq jobs status {job_id}`",
    }))


def build_parser():
    import argparse
    ap = argparse.ArgumentParser(
        prog="scq",
        description="Agent-friendly Spark Connect CLI: read-only querying + "
                    "async long-job control. No JVM, no Kerberos on the client.")
    ap.add_argument("--remote", default=DEFAULT_REMOTE,
                    help=f"Spark Connect endpoint (default {DEFAULT_REMOTE} / $SPARK_REMOTE)")
    ap.add_argument("--format", default="jsoneachrow",
                    choices=["jsoneachrow", "json", "csv", "tsv", "table"])
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("databases", help="List databases").set_defaults(func=query.cmd_databases)

    pt = sub.add_parser("tables", help="List tables in a database")
    pt.add_argument("database", nargs="?", default=None)
    pt.add_argument("--like", default=None)
    pt.set_defaults(func=query.cmd_tables)

    pd = sub.add_parser("describe", help="Show a table's columns")
    pd.add_argument("table")
    pd.set_defaults(func=query.cmd_describe)

    pq = sub.add_parser("query", help="Run SQL (read-only unless --allow-ddl)")
    pq.add_argument("sql")
    pq.add_argument("--allow-ddl", action="store_true")
    pq.add_argument("--max-rows", type=int, default=None)
    pq.set_defaults(func=query.cmd_query)

    ps = sub.add_parser("sync", help="Submit an async Hive->ClickHouse sync (returns a job id)")
    ps.add_argument("source", help="Hive table, e.g. db.table")
    ps.add_argument("--to", default="clickhouse")
    ps.add_argument("--mode", choices=["auto", "spark", "pipe"], default="auto")
    ps.add_argument("--ch-jdbc", default=None)
    ps.add_argument("--target", default=None, help="ClickHouse target table")
    ps.add_argument("--where", default=None)
    ps.add_argument("--limit", type=int, default=0)
    ps.set_defaults(func=cmd_sync)

    pj = sub.add_parser("jobs", help="Manage async jobs")
    js = pj.add_subparsers(dest="jcmd", required=True)
    js.add_parser("list", help="List jobs").set_defaults(func=jobs.cmd_list)
    s = js.add_parser("status", help="Show a job's full status")
    s.add_argument("id"); s.set_defaults(func=jobs.cmd_status)
    lg = js.add_parser("logs", help="Show a job's log (tail by default)")
    lg.add_argument("id"); lg.add_argument("--tail", type=int, default=40)
    lg.add_argument("--full", action="store_true"); lg.set_defaults(func=jobs.cmd_logs)
    c = js.add_parser("cancel", help="Cancel a running job")
    c.add_argument("id"); c.set_defaults(func=jobs.cmd_cancel)

    return ap


def main(argv=None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    # Internal entrypoint for the detached worker.
    if argv and argv[0] == "__run-job":
        jobs.run_worker(argv[1])
        return
    args = build_parser().parse_args(argv)
    args.func(args)
