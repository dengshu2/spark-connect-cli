"""Hive -> ClickHouse sync — one feature built on the async job subsystem.

Runs inside a detached job worker, so all output goes to the job log and never
into the agent's context. The data path is **Spark direct write**: a Spark
Connect job reads the Hive table and writes to ClickHouse over JDBC. The write
happens on the executors (in the cluster), so rows never pass through this
process or the agent.

Requirements (the "path A wiring"):
  - clickhouse-jdbc on the Spark Connect server classpath (/opt/spark/jars/),
  - network egress from the cluster to ClickHouse,
  - a JDBC URL with credentials (`--ch-jdbc` / $SCQ_CH_JDBC),
  - the target ClickHouse table already created with a suitable engine
    (Spark `append` does not create a usable MergeTree table for you).

Modes:
  single   — one JDBC connection (numPartitions=1). Best for small tables.
  parallel — N partitions write concurrently. Best for large tables.
  auto     — single under --auto-threshold rows, else parallel.
"""
from __future__ import annotations

import argparse
import os
import re

from .jobs import write_meta
from .session import DEFAULT_REMOTE, get_spark

# Spark/Hive -> ClickHouse type mapping. The SKILL carries the authoritative
# table the agent reasons with; this is just for the descriptive log line.
SPARK_TO_CH = {
    "boolean": "Bool", "tinyint": "Int8", "smallint": "Int16", "int": "Int32",
    "integer": "Int32", "bigint": "Int64", "float": "Float32", "double": "Float64",
    "string": "String", "varchar": "String", "char": "String", "binary": "String",
    "date": "Date32", "timestamp": "DateTime64(3)",
}

AUTO_THRESHOLD = int(os.environ.get("SCQ_AUTO_THRESHOLD", "1000000"))
DEFAULT_BATCHSIZE = int(os.environ.get("SCQ_BATCHSIZE", "100000"))
DEFAULT_NUM_PARTITIONS = int(os.environ.get("SCQ_NUM_PARTITIONS", "8"))


def map_type(spark_type: str) -> str:
    t = spark_type.lower().strip()
    if t.startswith("decimal"):
        return t.replace("decimal", "Decimal")
    base = re.split(r"[(<]", t, 1)[0]
    return SPARK_TO_CH.get(base, "String")


def _parse(argv: list[str]):
    p = argparse.ArgumentParser(prog="scq sync")
    p.add_argument("source")
    p.add_argument("--to", default="clickhouse")
    p.add_argument("--remote", default=DEFAULT_REMOTE)
    p.add_argument("--mode", choices=["auto", "parallel", "single"], default="auto")
    p.add_argument("--ch-jdbc", default=os.environ.get("SCQ_CH_JDBC", ""))
    p.add_argument("--target", default=None)
    p.add_argument("--where", default=None)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--batchsize", type=int, default=DEFAULT_BATCHSIZE)
    p.add_argument("--num-partitions", type=int, default=DEFAULT_NUM_PARTITIONS)
    p.add_argument("--auto-threshold", type=int, default=AUTO_THRESHOLD)
    return p.parse_args(argv)


def run(argv: list[str], meta: dict) -> int:
    a = _parse(argv)
    if not a.ch_jdbc:
        print("[scq] no --ch-jdbc / SCQ_CH_JDBC set — cannot write to ClickHouse",
              flush=True)
        return 2

    print(f"[scq] sync start: {a.source} -> {a.to}  mode={a.mode}", flush=True)
    spark = get_spark(a.remote)

    # 1. discover schema (informational; the target table must already exist)
    desc = spark.sql(f"DESCRIBE TABLE {a.source}").collect()
    cols = [(r[0], r[1]) for r in desc if r[0] and not r[0].startswith("#")]
    print(f"[scq] {len(cols)} columns: "
          + ", ".join(f"{c}:{t}->{map_type(t)}" for c, t in cols), flush=True)

    src_count = spark.sql(f"SELECT count(*) c FROM {a.source}").collect()[0]["c"]
    target = a.target or a.source.split(".")[-1]
    meta["source_rows"] = src_count
    meta["target"] = target
    write_meta(meta["id"], meta)
    print(f"[scq] source rows: {src_count} -> target {target}", flush=True)

    # 2. build the read
    sel = f"SELECT * FROM {a.source}"
    if a.where:
        sel += f" WHERE {a.where}"
    if a.limit:
        sel += f" LIMIT {a.limit}"
    df = spark.sql(sel)

    # 3. choose write parallelism
    mode = a.mode
    if mode == "auto":
        mode = "parallel" if src_count >= a.auto_threshold else "single"
    num_partitions = 1 if mode == "single" else max(1, a.num_partitions)
    if num_partitions == 1:
        df = df.coalesce(1)
    else:
        df = df.repartition(num_partitions)
    print(f"[scq] writing via JDBC: mode={mode} numPartitions={num_partitions} "
          f"batchsize={a.batchsize}", flush=True)

    # 4. Spark direct write to ClickHouse. Rows are written by the executors;
    # nothing flows through this process.
    try:
        (df.write.format("jdbc")
           .option("url", a.ch_jdbc)
           .option("dbtable", target)
           .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
           .option("batchsize", a.batchsize)
           .option("isolationLevel", "NONE")  # ClickHouse has no txns
           .mode("append").save())
    except Exception as e:  # noqa: BLE001
        print(f"[scq] JDBC write failed: {e}", flush=True)
        return 1

    print(f"[scq] done: wrote {src_count} rows to {target}", flush=True)
    meta["written_rows"] = src_count
    write_meta(meta["id"], meta)
    return 0
