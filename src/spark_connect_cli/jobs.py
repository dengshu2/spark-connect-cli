"""Layer A — async background-job control.

The point: long Spark jobs must not block the caller (an LLM agent should not sit
in a 30-minute tool call). `submit()` spawns the work detached in its own process
group, records a durable handle on disk, and returns immediately. The handle
survives a process/container restart because it lives in a file registry, not in
memory.

A job is generic: `kind="exec"` runs an arbitrary argv (used for tests and ad-hoc
long commands); `kind="sync"` runs the Hive->ClickHouse mover. New kinds plug in
via `_dispatch`.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .session import EXIT_JOB_ERR, err

JOBS_DIR = Path(os.environ.get(
    "SCQ_JOBS_DIR", str(Path.home() / ".spark-connect-cli" / "jobs")))


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def _meta_path(job_id: str) -> Path:
    return _job_dir(job_id) / "meta.json"


def _log_path(job_id: str) -> Path:
    return _job_dir(job_id) / "out.log"


def read_meta(job_id: str) -> dict:
    p = _meta_path(job_id)
    if not p.exists():
        err(f"no such job: {job_id}", EXIT_JOB_ERR)
    return json.loads(p.read_text())


def write_meta(job_id: str, meta: dict) -> None:
    tmp = _meta_path(job_id).with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=2, default=str))
    tmp.replace(_meta_path(job_id))


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # Signal-0 succeeds for a zombie (terminated but not yet reaped by its
    # parent), but a zombie is not doing work — treat it as dead. On Linux the
    # process state is the char right after the ")" that closes comm in stat.
    try:
        with open(f"/proc/{pid}/stat") as f:
            stat = f.read()
        if stat[stat.rfind(")") + 2] == "Z":
            return False
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return False
    return True


def reconcile(meta: dict) -> dict:
    """A 'running' job whose process is gone but that never recorded an end has
    crashed — mark it failed so status never lies."""
    if meta.get("state") == "running" and not _pid_alive(meta.get("pid", 0)):
        meta["state"] = "failed"
        meta["ended_at"] = _now()
        if meta.get("exit_code") is None:
            meta["exit_code"] = -1
        meta["error"] = meta.get("error") or "process exited without recording completion"
        write_meta(meta["id"], meta)
    return meta


def _new_job_id() -> str:
    return f"j-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def submit(kind: str, argv: list[str], descr: dict | None = None) -> str:
    """Spawn a detached worker for this job and return its id immediately."""
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    job_id = _new_job_id()
    _job_dir(job_id).mkdir(parents=True)
    meta = {
        "id": job_id, "kind": kind, "state": "submitted",
        "submitted_at": _now(), "started_at": None, "ended_at": None,
        "pid": None, "pgid": None, "exit_code": None, "argv": argv,
        **(descr or {}),
    }
    write_meta(job_id, meta)

    log = open(_log_path(job_id), "ab", buffering=0)
    proc = subprocess.Popen(
        [sys.executable, "-m", "spark_connect_cli", "__run-job", job_id],
        stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        start_new_session=True,  # own process group -> kill the whole tree
        env=os.environ.copy(),
    )
    meta["pid"] = proc.pid
    meta["pgid"] = os.getpgid(proc.pid)
    meta["state"] = "running"
    meta["started_at"] = _now()
    write_meta(job_id, meta)
    return job_id


def _dispatch(meta: dict) -> int:
    kind = meta["kind"]
    argv = meta.get("argv", [])
    if kind == "exec":
        return subprocess.run(argv).returncode
    if kind == "sync":
        from .sync import run as sync_run
        return sync_run(argv, meta)
    print(f"[scq] unknown job kind: {kind}", flush=True)
    return 2


def run_worker(job_id: str) -> None:
    """Runs INSIDE the detached child. Executes the work, then records a terminal
    state. Never raises out — always writes a final meta."""
    meta = read_meta(job_id)
    rc = 0
    try:
        rc = _dispatch(meta)
    except SystemExit as e:
        rc = int(e.code) if isinstance(e.code, int) else 1
    except Exception as e:  # noqa: BLE001
        meta = read_meta(job_id)
        meta["error"] = str(e)
        print(f"[scq] job failed: {e}", flush=True)
        rc = 1
    meta = read_meta(job_id)  # re-read: the body may have updated counters
    meta["state"] = "succeeded" if rc == 0 else "failed"
    meta["exit_code"] = rc
    meta["ended_at"] = _now()
    write_meta(job_id, meta)
    sys.exit(rc)


# -- agent-facing commands -------------------------------------------------

def cmd_list(args) -> None:
    if not JOBS_DIR.exists():
        print(json.dumps({"jobs": []}))
        return
    out = []
    for d in sorted(JOBS_DIR.iterdir(), reverse=True):
        if not (d / "meta.json").exists():
            continue
        m = reconcile(json.loads((d / "meta.json").read_text()))
        out.append({k: m.get(k) for k in
                    ("id", "kind", "state", "source", "target", "submitted_at",
                     "source_rows", "written_rows")})
    if args.format == "table":
        from .session import emit_rows
        emit_rows([(j["id"], j["kind"], j["state"], j.get("source") or "",
                    str(j.get("written_rows") if j.get("written_rows") is not None else ""))
                   for j in out],
                  ["id", "kind", "state", "source", "written"], "table")
    else:
        print(json.dumps({"jobs": out}, default=str))


def cmd_status(args) -> None:
    print(json.dumps(reconcile(read_meta(args.id)), indent=2, default=str))


def cmd_logs(args) -> None:
    read_meta(args.id)  # validate existence
    p = _log_path(args.id)
    if not p.exists():
        print("")
        return
    data = p.read_text(errors="replace")
    if args.full:
        sys.stdout.write(data)
        return
    lines = data.splitlines()
    sys.stdout.write("\n".join(lines[-args.tail:]) + ("\n" if lines else ""))


def cmd_cancel(args) -> None:
    meta = reconcile(read_meta(args.id))
    if meta["state"] not in ("running", "submitted"):
        print(json.dumps({"id": args.id, "state": meta["state"],
                          "message": "job already finished"}))
        return
    pgid = meta.get("pgid")
    if pgid:
        try:
            os.killpg(pgid, signal.SIGTERM)
            time.sleep(2)
            if _pid_alive(meta.get("pid", 0)):
                os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    meta["state"] = "cancelled"
    meta["ended_at"] = _now()
    write_meta(args.id, meta)
    print(json.dumps({"id": args.id, "state": "cancelled"}))
