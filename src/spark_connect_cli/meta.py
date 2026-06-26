"""scq meta — one structured metadata document for a table.

Bundles what is otherwise spread across DESCRIBE EXTENDED + SHOW PARTITIONS +
the Spark `_metadata` hidden column, so an agent gets the whole picture of a
table in a single call instead of stitching several queries.
"""
from __future__ import annotations

import json

from .session import EXIT_QUERY_ERR, err, get_spark
from .sync import map_type


def _describe_extended(spark, table):
    """Parse DESCRIBE EXTENDED into (columns, partition_columns, details)."""
    rows = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
    cols, part_cols, details = [], [], {}
    section = "cols"
    for r in rows:
        name = (r[0] or "").strip()
        val = (r[1] or "")
        if name.startswith("# Partition Information"):
            section = "partcols"
            continue
        if name.startswith("# Detailed Table Information"):
            section = "details"
            continue
        if name.startswith("#") or not name:
            continue
        if section == "cols":
            cols.append({"name": name, "type": val, "clickhouse": map_type(val),
                         "comment": r[2]})
        elif section == "partcols":
            part_cols.append(name)
        else:
            details[name] = val.strip()
    return cols, part_cols, details


def _file_stats(spark, table):
    """Aggregate per-file size/mtime from the _metadata hidden column. Returns
    None if the source doesn't expose _metadata."""
    try:
        row = spark.sql(
            f"SELECT count(*) AS num_files, sum(sz) AS total_bytes, "
            f"min(mt) AS first_modified, max(mt) AS last_modified FROM ("
            f"  SELECT _metadata.file_path AS p, max(_metadata.file_size) AS sz, "
            f"  max(_metadata.file_modification_time) AS mt FROM {table} GROUP BY 1)"
        ).collect()[0]
        return {"numFiles": row["num_files"], "totalBytes": row["total_bytes"],
                "firstModified": str(row["first_modified"]),
                "lastModified": str(row["last_modified"])}
    except Exception:  # noqa: BLE001 — _metadata unsupported / empty table
        return None


def cmd_meta(args) -> None:
    spark = get_spark(args.remote)
    table = args.table
    try:
        cols, part_cols, details = _describe_extended(spark, table)
    except Exception as e:  # noqa: BLE001
        err(f"describe failed: {e}", EXIT_QUERY_ERR)

    out = {
        "table": table,
        "createdTime": details.get("Created Time"),
        "lastAccess": details.get("Last Access"),
        "owner": details.get("Owner"),
        "createdBy": details.get("Created By"),
        "provider": details.get("Provider"),
        "type": details.get("Type"),
        "location": details.get("Location"),
        "statistics": details.get("Statistics"),
        "partitionColumns": part_cols,
        "columns": cols,
    }

    if part_cols:
        try:
            parts = [r[0] for r in spark.sql(f"SHOW PARTITIONS {table}").collect()]
            out["partitionCount"] = len(parts)
            out["partitions"] = parts if len(parts) <= 200 else parts[:200] + ["…"]
        except Exception:  # noqa: BLE001
            pass

    files = _file_stats(spark, table)
    if files:
        out["files"] = files

    if args.count:
        out["rowCount"] = spark.sql(f"SELECT count(*) c FROM {table}").collect()[0]["c"]

    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
