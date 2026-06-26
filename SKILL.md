---
name: spark-connect-cli
description: >-
  Query Spark / Hive from the shell with the `scq` CLI over Spark Connect, and
  run long Spark jobs (e.g. Hive->ClickHouse syncs) without blocking. Use
  whenever the user wants to read Hive/Spark data, explore databases/tables/
  schema, run a Spark SQL analysis, or sync a Hive table somewhere. Triggers:
  Hive, Spark, Spark SQL, 查 Hive, 跑个 Spark SQL, 看下这个表, 同步到 ClickHouse,
  sync table.
---

# scq — Spark Connect from the shell

`scq` queries a Spark Connect server (JSON-first, **read-only by default**) and
manages **async long jobs** so you never sit in a blocking tool call.

## Discover before you query

Don't guess names. Discover them first:

1. `scq databases` — list databases.
2. `scq tables [DB] --like '%keyword%'` — list tables.
3. `scq describe <db.table>` — list columns (name, type, comment).
4. `scq query "SELECT ..."` — run it once you know the schema.

## Reading output

- **stdout** carries data. Default is **JSONEachRow** (NDJSON — one JSON object
  per line). Other formats: `--format json|csv|tsv|table`.
- **stderr** carries errors as one JSON object `{"error": ..., "code": ...}`.
- `query` caps at `SCQ_MAX_ROWS` (default 10k); a `{"warning": ...}` on stderr
  means it was truncated — add `LIMIT`/filters or raise `--max-rows`.

## Branch on the exit code

`0` ok · `1` query error (fix the SQL) · `2` connection error (check
`$SPARK_REMOTE`) · `3` read-only guard blocked it · `4` job-control error.

## Read-only by default

`scq query` allows only SELECT/SHOW/DESCRIBE/EXPLAIN/WITH. Writes and DDL exit
with code `3` unless you pass `--allow-ddl`. **Only** add `--allow-ddl` when the
user explicitly asked to modify data or schema.

## Long jobs — submit, then poll. NEVER block.

A full-table sync is a multi-minute Spark job. **Do not** run it in the
foreground and wait. Submit it, tell the user the job id, and hand control back:

```bash
scq sync ods.orders --to clickhouse      # prints {"job_id": "...", "state": "running"}
```

Then, *only when the user asks* "how's it going" / after a natural pause:

```bash
scq jobs status j-20260625-...           # state, source_rows, written_rows, exit_code
scq jobs logs   j-20260625-... --tail 40 # recent progress
scq jobs cancel j-20260625-...           # stop it
```

Etiquette:
- After submitting, reply with the job id and a one-line "I'll check when you
  want." Don't loop on `status` in a tight wait — let the user drive, or poll on
  a relaxed cadence.
- Report terminal state plainly: `succeeded` with `written_rows`, or `failed`
  with the tail of the log.

## Hive → ClickHouse sync workflow

When the user says "同步 X 表到 ClickHouse":

1. `scq describe <src>` — get the Hive schema.
2. Decide the **target database and table**. `--target` takes `db.table`:
   - If the user names a database (e.g. `class_db`), pass it **qualified**:
     `--target class_db.class`. A **bare table name lands in the connection's
     default database** (`default`) — don't let data silently go there.
   - The **database must already exist** (auto-create makes the table, not the
     database). Ensure it first: `chsql query --allow-ddl "CREATE DATABASE IF NOT
     EXISTS class_db"`.
3. Make sure the target table is good:
   - For a quick/one-off sync, let `scq sync` auto-create it — but pass
     `--order-by <key>` so it gets a real sort key (otherwise it is created with
     `ORDER BY tuple()`, no primary index).
   - For a production table, **pre-create it** with `chsql query --allow-ddl
     "CREATE TABLE class_db.class (...) ENGINE = MergeTree ORDER BY (...)"` (full
     control over engine, keys, partitioning), then sync.
4. Submit: `scq sync <src> --target db.table [--order-by key] [--where ...]`.
5. Hand back the job id. Verify with row counts when it finishes.

The ClickHouse JDBC connection (`$SCQ_CH_JDBC`) is preconfigured — you do **not**
pass credentials; just choose the `db.table` with `--target`.

### Spark/Hive → ClickHouse type mapping

| Spark/Hive | ClickHouse |
|------------|------------|
| boolean | Bool |
| tinyint / smallint / int / bigint | Int8 / Int16 / Int32 / Int64 |
| float / double | Float32 / Float64 |
| decimal(p,s) | Decimal(p,s) |
| string / varchar / char / binary | String |
| date | Date32 |
| timestamp | DateTime64(3) |

Nullable columns map to `Nullable(T)`. Nested/complex types default to `String`
(JSON) — confirm with the user before relying on them.

## Metadata & execution introspection

Two general primitives — don't hand-stitch many queries.

**Table metadata → `scq meta db.table`** — one JSON: schema (+ ClickHouse type
mapping), created time, owner, format, HDFS location, partition columns +
partition list/count, file count/total size, and min/max file modification time
(i.e. "when did the data arrive"). Add `--count` for an exact row count (runs a
`count(*)`, so only when asked). For ad-hoc bits you can still use
`scq query "DESCRIBE EXTENDED t"` / `"SHOW PARTITIONS t"`.

**Execution metadata → `scq exec <path>`** — read-only passthrough to the Spark
REST API (auto-discovers the app, GET-only). The model reads the JSON, so any
runtime question is the same command with a different path:

```bash
scq exec stages?status=active          # what's running now
scq exec sql                           # each query's plan + metrics
scq exec executors                     # cores / memory / GC / shuffle
scq exec jobs
scq exec stages/<id>/<attempt>/taskSummary?quantiles=0.5,0.95,1.0
```

- **`executors` memory**: `maxMemory` / `memoryUsed` are the **storage/cache
  pool** (roughly `(heap − 300MB) × 0.6`), **not** the executor's total memory.
  A ~100MB `maxMemory` does **not** mean a tiny executor — total heap is set by
  `spark.executor.memory`. Don't report the cache pool as the executor size.
- **Data skew**: pull a stage's `taskSummary` and compare a metric's **max vs
  median** (`executorRunTime`, `shuffleReadBytes`, `shuffleReadRecords`). A large
  `max/median` ratio = a straggler / skewed partition. `…?details=true` on a
  stage lists every task to find the hot one.
- Stage/job lists can be long — filter (`?status=active`) or fetch one id.
- For the *plan before running*, use `scq query "EXPLAIN FORMATTED SELECT ..."`.

## Connection

`scq --remote sc://host:15002 ...` or set `$SPARK_REMOTE`. No Kerberos or JVM is
needed on this side — the Spark Connect server does the auth.

## Recipes

```bash
scq --format table tables analytics --like '%event%'
scq query --format table "SELECT count(*) FROM analytics.events"
scq query --max-rows 0 "SELECT * FROM small_dim"        # no cap
scq sync analytics.events --to clickhouse --where "dt='2026-06-25'"
```
