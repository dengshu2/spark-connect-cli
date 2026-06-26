# spark-connect-cli (`scq`)

An agent-friendly [Spark Connect](https://spark.apache.org/spark-connect/) CLI —
**read-only querying** plus **async control for long-running jobs**.

Built for LLM agents and humans who live in a shell. Unlike `spark-sql` /
`spark-submit`, the client is a thin **pure-Python gRPC client**: no JVM, and
**no Kerberos on the client side** — the Spark Connect server authenticates with
its own keytab, so you just point at `sc://host:15002` and go.

## Why

- **JSON-first, read-only by default.** Safe for an agent to call for
  exploration; writes/DDL are blocked unless you opt in (`--allow-ddl`).
- **Long jobs don't block you.** A multi-minute Spark job shouldn't trap an agent
  in a 30-minute tool call. `scq` submits the job, hands back a durable **job
  id**, and returns immediately. Poll it whenever you like; the handle survives a
  client/container restart because it lives in an on-disk registry.
- **Stable exit codes** so a caller can branch without scraping text.

## Install

```bash
pip install spark-connect-cli         # once published
# or, from source:
pip install -e .
```

## Quick start

```bash
export SPARK_REMOTE=sc://localhost:15002   # your Spark Connect endpoint

scq databases
scq tables mydb --like '%orders%'
scq describe mydb.orders
scq query "SELECT id, name FROM mydb.orders LIMIT 10"
```

Output is **JSONEachRow** (one JSON object per line) by default; pick another with
`--format json|csv|tsv|table`.

### Read-only guard

`scq query` allows only `SELECT/SHOW/DESCRIBE/EXPLAIN/WITH`. Anything else exits
with code **3** unless you pass `--allow-ddl`.

| exit | meaning |
|------|---------|
| 0 | success |
| 1 | query error (bad SQL) |
| 2 | connection error |
| 3 | blocked by the read-only guard |
| 4 | job-control error (no such job, …) |

## Async jobs (Layer A)

Long work runs detached and is tracked by a file-based registry under
`$SCQ_JOBS_DIR` (default `~/.spark-connect-cli/jobs`).

```bash
# submit — returns a job id immediately, does NOT block
scq sync ods.orders --to clickhouse
# {"job_id": "j-20260625-...", "state": "running", "message": "... poll with ..."}

scq jobs list                       # all jobs + state
scq jobs status j-20260625-...      # full status (rows, timings, pid, exit code)
scq jobs logs   j-20260625-... --tail 40
scq jobs cancel j-20260625-...      # kills the whole process group
```

Design: each job is a directory with `meta.json` (state machine:
`submitted → running → succeeded|failed|cancelled`) and `out.log`. The worker
runs in its **own process group**, so cancel kills the entire tree (no orphans).
A `running` job whose process has vanished is reconciled to `failed` on the next
status read, so status never lies.

## Hive → ClickHouse sync

`scq sync` is one job kind built on the async subsystem. It uses **Spark direct
write**: a Spark Connect job reads the Hive table and writes to ClickHouse over
JDBC. The write runs on the executors, so rows never pass through this process or
the agent.

Modes control write parallelism — `single` (one connection, small tables),
`parallel` (N partitions, large tables), `auto` (picks by row count).

Requires:
- `clickhouse-jdbc` on the Spark Connect server classpath (`/opt/spark/jars/`),
- cluster→ClickHouse network egress,
- a JDBC URL with credentials via `--ch-jdbc` / `$SCQ_CH_JDBC`,
- the **target ClickHouse table created beforehand** with a suitable engine
  (Spark `append` won't build a usable MergeTree table for you — create it first,
  e.g. with the `chsql` skill).

## Introspection

```bash
scq meta db.table            # one JSON: schema, created time, location,
                             # partitions, file count/size, mtime range
scq meta db.table --count    # also run an exact count(*)

scq exec stages?status=active            # read-only Spark REST passthrough
scq exec executors
scq exec stages/<id>/<attempt>/taskSummary?quantiles=0.5,0.95,1.0   # skew: max/median
```

`scq exec` auto-discovers the running Spark app via the YARN ResourceManager and
proxies its monitoring REST API (GET-only). Set the RM base with `$SCQ_YARN_RM`.

**Reading `scq exec executors`** — the `maxMemory` field is Spark's
**storage/cache pool** (`(heap − 300 MB reserved) × 0.6`), *not* the executor's
total memory: a 512 MB executor reports ~93 MB, a 1536 MB driver ~741 MB. The
real heap is `spark.executor.memory` (+ off-heap overhead). The `driver` row has
0 cores and runs no tasks. With dynamic allocation, idle executors are released —
so the list may show only the driver when nothing is running.

## Configuration

| env | default | meaning |
|-----|---------|---------|
| `SPARK_REMOTE` | `sc://localhost:15002` | Spark Connect endpoint |
| `SCQ_JOBS_DIR` | `~/.spark-connect-cli/jobs` | job registry (put on a persistent volume) |
| `SCQ_MAX_ROWS` | `10000` | default row cap for `query` |
| `SCQ_CH_JDBC` | — | ClickHouse JDBC URL for `sync` path A |
| `SCQ_YARN_RM` | `http://namenode.hive-net:8088` | YARN RM base for `scq exec` |

## Use with an LLM agent

`SKILL.md` ships a ready-made skill (discover-before-query workflow, async-job
etiquette, type-mapping table). Drop it into your agent's skills directory and
the agent drives `scq` through a shell/Bash tool.

## License

MIT
