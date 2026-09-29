"""Local TCP runner.

Local mode intentionally uses the same coordinator/worker protocol as Slurm
mode.  The only difference is process placement: the coordinator binds to
localhost and worker processes are spawned directly instead of submitted through
Slurm.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import json
import time
from pathlib import Path

from . import config
from .coordinator import run_server
from .worker import run_network_worker


def local_process_count() -> int:
    slurm_cpus = os.environ.get("SLURM_CPUS_PER_TASK")
    if config.LOCAL_PROCESS_COUNT is not None:
        requested = max(1, int(config.LOCAL_PROCESS_COUNT))
    else:
        requested = max(1, (os.cpu_count() or 2) - 1)
    if slurm_cpus:
        return min(requested, max(1, int(slurm_cpus) - 1))
    return requested


def choose_local_port() -> int:
    if config.LOCAL_COORDINATOR_PORT:
        return int(config.LOCAL_COORDINATOR_PORT)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((config.LOCAL_COORDINATOR_HOST, 0))
        return int(sock.getsockname()[1])


def wait_for_port(host: str, port: int, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1) as sock:
                sock.sendall(b'{"type":"STATUS","worker_id":"local-launcher"}\n')
                data = b""
                while not data.endswith(b"\n"):
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                if data and json.loads(data.decode("utf-8")).get("ok"):
                    return
                raise RuntimeError(f"unexpected coordinator response: {data!r}")
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.1)
    raise RuntimeError(f"local coordinator did not start on {host}:{port}: {last_error}")


def coordinator_request(host: str, port: int, payload: dict) -> dict:
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.sendall((json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
        data = b""
        while not data.endswith(b"\n"):
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
    if not data:
        raise RuntimeError("local coordinator returned no data")
    response = json.loads(data.decode("utf-8"))
    if not response.get("ok"):
        raise RuntimeError(response.get("error", "local coordinator request failed"))
    return response


def _run_coordinator(run_id: str, total_steps: int, db_path: str, host: str, port: int) -> None:
    run_server(
        run_id,
        total_steps,
        Path(db_path),
        host,
        port,
        auto_exit_when_done=False,
    )


def _run_worker(worker_id: str, host: str, port: int, run_id: str) -> None:
    run_network_worker(worker_id, host, port, idle_exit_seconds=0, run_id=run_id)


def run_local(run_id: str, total_steps: int, *, process_count: int | None = None) -> None:
    run_config_path = config.RUNS_ROOT / run_id / "run_config.json"
    if not run_config_path.is_file():
        raise RuntimeError(f"local_runner requires persisted run metadata: {run_config_path}")
    run_config = json.loads(run_config_path.read_text(encoding="utf-8"))
    if "graph_edges" not in run_config:
        raise RuntimeError(f"run metadata is missing graph_edges: {run_config_path}")
    config.validate_entrypoint_runtime(
        operation="local_runner",
        require_compute_node=True,
        worker_count=0,
        edges=run_config.get("graph_edges"),
    )
    host = config.LOCAL_COORDINATOR_HOST
    port = choose_local_port()
    db_path = config.RUNS_ROOT / run_id / "coordinator.sqlite"
    if process_count is None:
        process_count = local_process_count()
    else:
        process_count = max(1, int(process_count))
        slurm_cpus = os.environ.get("SLURM_CPUS_PER_TASK")
        if slurm_cpus:
            process_count = min(process_count, max(1, int(slurm_cpus) - 1))

    coordinator = mp.Process(
        target=_run_coordinator,
        args=(run_id, total_steps, str(db_path), host, port),
        name=f"gd-local-coordinator-{run_id}",
    )
    coordinator.start()
    try:
        wait_for_port(host, port)
        workers = [
            mp.Process(
                target=_run_worker,
                args=(f"local-{idx + 1}", host, port, run_id),
                name=f"gd-local-worker-{idx + 1}",
            )
            for idx in range(process_count)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode != 0:
                raise RuntimeError(f"{worker.name} exited with status {worker.exitcode}")
        if coordinator.is_alive():
            coordinator_request(host, port, {"type": "SHUTDOWN", "worker_id": "local-launcher"})
        coordinator.join(timeout=30)
        if coordinator.is_alive():
            raise RuntimeError("local coordinator did not exit after workers completed")
        if coordinator.exitcode != 0:
            raise RuntimeError(f"local coordinator exited with status {coordinator.exitcode}")
    finally:
        if coordinator.is_alive():
            coordinator.terminate()
            coordinator.join(timeout=5)
