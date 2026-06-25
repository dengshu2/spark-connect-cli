"""Layer A async job lifecycle — no Spark required (uses kind='exec')."""
import importlib
import json
import os
import time

import pytest


@pytest.fixture()
def jobs(tmp_path, monkeypatch):
    # The detached worker is a fresh subprocess; it reads SCQ_JOBS_DIR from the
    # environment, so we must set it (not just patch the module global).
    monkeypatch.setenv("SCQ_JOBS_DIR", str(tmp_path / "jobs"))
    import spark_connect_cli.jobs as jobs_mod
    importlib.reload(jobs_mod)
    return jobs_mod


def _wait(jobs, job_id, *, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = jobs.reconcile(jobs.read_meta(job_id))
        if m["state"] in ("succeeded", "failed", "cancelled"):
            return m
        time.sleep(0.2)
    raise AssertionError(f"job {job_id} did not finish in {timeout}s")


def test_submit_returns_immediately_and_succeeds(jobs):
    t0 = time.time()
    job_id = jobs.submit("exec", ["sh", "-c", "sleep 2; echo hello-from-job"])
    # submit must not block for the job's duration
    assert time.time() - t0 < 1.0
    m = jobs.reconcile(jobs.read_meta(job_id))
    assert m["state"] in ("running", "submitted")

    m = _wait(jobs, job_id)
    assert m["state"] == "succeeded"
    assert m["exit_code"] == 0
    log = (jobs.JOBS_DIR / job_id / "out.log").read_text()
    assert "hello-from-job" in log


def test_failure_is_recorded(jobs):
    job_id = jobs.submit("exec", ["sh", "-c", "echo boom; exit 7"])
    m = _wait(jobs, job_id)
    assert m["state"] == "failed"
    assert m["exit_code"] == 7


def test_cancel_kills_process_group(jobs):
    job_id = jobs.submit("exec", ["sh", "-c", "sleep 60"])
    time.sleep(0.5)
    pid = jobs.read_meta(job_id)["pid"]
    assert jobs._pid_alive(pid)

    class A:
        id = job_id
    jobs.cmd_cancel(A())

    m = jobs.read_meta(job_id)
    assert m["state"] == "cancelled"
    time.sleep(0.5)
    assert not jobs._pid_alive(pid)


def test_unknown_job_is_an_error(jobs):
    with pytest.raises(SystemExit) as e:
        jobs.read_meta("nope")
    assert e.value.code == 4  # EXIT_JOB_ERR
