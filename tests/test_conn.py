"""Connection preflight — a dead endpoint must fail fast with EXIT_CONN_ERR
instead of letting gRPC retry for minutes (which would trap the caller)."""
import socket
import time

import pytest

from spark_connect_cli.session import get_spark


def _closed_port() -> int:
    # Bind to an ephemeral port, then close it so a connect() is refused.
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_dead_endpoint_fails_fast_with_code_2(monkeypatch):
    monkeypatch.setenv("SCQ_CONNECT_TIMEOUT", "3")
    port = _closed_port()
    t0 = time.time()
    with pytest.raises(SystemExit) as e:
        get_spark(f"sc://127.0.0.1:{port}")
    assert e.value.code == 2  # EXIT_CONN_ERR
    assert time.time() - t0 < 3.0  # refused immediately, well under the timeout
