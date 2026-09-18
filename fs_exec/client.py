from __future__ import annotations

import os
import shutil
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO

from .config import Target
from .latency import LatencyStore
from .protocol import TargetPaths, new_job_id, publish_cancel, publish_request, read_final
from .util import canonical_json, exact_wait, now_ns, read_json, sha256_file, write_exclusive


@dataclass(frozen=True)
class JobStatus:
    job_id: str
    state: str
    watcher_state: str
    result: dict[str, Any] | None = None


class Client:
    def __init__(self, target: Target, *, state_dir: Path | None = None) -> None:
        self.target = target
        self.paths = TargetPaths(target.path)
        state_dir = state_dir or Path.home() / ".local" / "state" / "fs-exec"
        self.latency = LatencyStore(state_dir / f"latency-{target.name}.jsonl")

    def submit(
        self,
        *,
        argv: list[str] | None = None,
        script: str | None = None,
        runtime: str | None = None,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
        stdin_file: Path | None = None,
        uploads: list[Path] | None = None,
        artifacts: list[str] | None = None,
        command_timeout: float = 60,
        expiry: float = 3600,
        job_id: str | None = None,
    ) -> str:
        job_id = job_id or new_job_id()
        manifest: dict[str, Any] = {
            "job_id": job_id,
            "mode": "shell" if script is not None else "argv",
            "cwd": cwd,
            "env": env or {},
            "command_timeout": command_timeout,
            "expires_ns": now_ns() + int(expiry * 1e9),
            "artifacts": artifacts or [],
        }
        if script is not None:
            manifest.update({"script": script, "runtime": runtime or ("powershell" if self.target.platform == "windows" else "bash")})
        else:
            manifest["argv"] = argv or []
        files = [(path, path.name) for path in uploads or []]
        if stdin_file:
            files.append((stdin_file, stdin_file.name))
            manifest["stdin_upload"] = stdin_file.name
        elif stdin is not None:
            manifest["stdin"] = stdin
        return publish_request(self.paths, manifest, files)

    def status(self, job_id: str) -> JobStatus:
        result_dir = self.paths.result(job_id)
        heartbeat = self._heartbeat()
        watcher_state = heartbeat.get("state", "UNKNOWN") if heartbeat else "UNKNOWN"
        heartbeat_stale = not heartbeat or (now_ns() - int(heartbeat.get("at_ns", 0))) > 15_000_000_000
        if (result_dir / "FINAL").exists():
            result = read_final(result_dir)
            state = "COMPLETED" if result.get("status") == "COMPLETED" else str(result.get("status"))
            return JobStatus(job_id, state, watcher_state, result)
        if (result_dir / "result.json").exists():
            return JobStatus(job_id, "RESULT_PROPAGATING", watcher_state)
        if (result_dir / "RUNNING").exists():
            return JobStatus(job_id, "AVAILABILITY_UNKNOWN" if heartbeat_stale else "RUNNING", watcher_state)
        if (result_dir / "CLAIMED").exists():
            return JobStatus(job_id, "AVAILABILITY_UNKNOWN" if heartbeat_stale else "CLAIMED", watcher_state)
        if self.paths.ready(job_id).joinpath("COMMIT").exists():
            return JobStatus(job_id, "AVAILABILITY_UNKNOWN" if heartbeat_stale else "VISIBLE_UNCLAIMED", watcher_state)
        return JobStatus(job_id, "AWAITING_VISIBILITY", watcher_state)

    def wait(self, job_id: str, total_timeout: float, *, stream: bool = False, stdout: BinaryIO | None = None, stderr: BinaryIO | None = None, result_timeout: float | None = None) -> JobStatus:
        deadline = time.monotonic() + total_timeout
        propagation_deadline: float | None = None
        stdout = stdout or sys.stdout.buffer
        stderr = stderr or sys.stderr.buffer
        counts = {"stdout": 0, "stderr": 0}
        while True:
            if stream:
                for name, output in (("stdout", stdout), ("stderr", stderr)):
                    while True:
                        chunk = self.paths.result(job_id) / name / f"{counts[name]:08d}.chunk"
                        if not chunk.exists():
                            break
                        output.write(chunk.read_bytes())
                        output.flush()
                        counts[name] += 1
            status = self.status(job_id)
            if status.result is not None:
                if stream:
                    final_propagation_deadline = time.monotonic() + result_timeout if result_timeout is not None else deadline
                    for name, output in (("stdout", stdout), ("stderr", stderr)):
                        expected = int(status.result.get(f"{name}_chunks", 0))
                        while counts[name] < expected:
                            chunk = self.paths.result(job_id) / name / f"{counts[name]:08d}.chunk"
                            visibility_budget = min(deadline, final_propagation_deadline) - time.monotonic()
                            if not exact_wait(chunk, visibility_budget):
                                return JobStatus(job_id, "RESULT_PROPAGATING", status.watcher_state)
                            output.write(chunk.read_bytes())
                            output.flush()
                            counts[name] += 1
                return status
            if status.state == "RESULT_PROPAGATING" and propagation_deadline is None and result_timeout is not None:
                propagation_deadline = time.monotonic() + result_timeout
            remaining = deadline - time.monotonic()
            if propagation_deadline is not None:
                remaining = min(remaining, propagation_deadline - time.monotonic())
            if remaining <= 0:
                return JobStatus(job_id, status.state, status.watcher_state)
            time.sleep(min(0.15, remaining))

    def cancel(self, job_id: str) -> None:
        publish_cancel(self.paths, job_id)

    def wait_for_phase(self, job_id: str, marker: str, timeout: float) -> bool:
        marker_paths = {
            "visibility": self.paths.ready(job_id) / "COMMIT",
            "claim": self.paths.result(job_id) / "CLAIMED",
            "start": self.paths.result(job_id) / "RUNNING",
            "result": self.paths.result(job_id) / "FINAL",
        }
        if marker not in marker_paths:
            raise ValueError(f"unknown phase: {marker}")
        return exact_wait(marker_paths[marker], timeout)

    def download_artifacts(self, job_id: str, destination: Path, visibility_timeout: float = 30) -> list[Path]:
        result = read_final(self.paths.result(job_id))
        copied: list[Path] = []
        for artifact in result.get("artifacts", []):
            relative = Path(str(artifact["name"]))
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"unsafe artifact path: {relative}")
            source = self.paths.result(job_id) / "artifacts" / relative
            if not exact_wait(source, visibility_timeout):
                raise ValueError(f"artifact was not visible before transport timeout: {relative}")
            if source.stat().st_size != int(artifact["size"]) or sha256_file(source) != artifact["sha256"]:
                raise ValueError(f"artifact checksum mismatch: {relative}")
            output = destination / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, output)
            copied.append(output)
        return copied

    def _heartbeat(self) -> dict[str, Any] | None:
        try:
            return read_json(self.paths.health / "heartbeat.json")
        except (OSError, ValueError):
            return None

    def health(self, *, probe: bool = True, timeout: float = 30) -> dict[str, Any]:
        self.paths.initialize()
        heartbeat = self._heartbeat()
        request_visibility = result_visibility = None
        if probe:
            probe_id = f"p-{uuid.uuid4().hex}"
            staging = self.paths.health / "pings" / f"{probe_id}.staging"
            ready = self.paths.health / "pings" / f"{probe_id}.ready"
            staging.mkdir()
            sent_ns = now_ns()
            write_exclusive(staging / "PING", canonical_json({"probe_id": probe_id, "client_ns": sent_ns}))
            os.replace(staging, ready)
            pong_path = self.paths.health / "pongs" / probe_id
            if exact_wait(pong_path, timeout):
                received_ns = now_ns()
                pong = read_json(pong_path)
                request_visibility = max(0.0, (int(pong["observed_ns"]) - sent_ns) / 1e9)
                result_visibility = max(0.0, (received_ns - int(pong.get("responded_ns", pong["observed_ns"]))) / 1e9)
                self.latency.record(request_visibility, result_visibility)
        stats = asdict(self.latency.stats())
        age = (now_ns() - int(heartbeat.get("at_ns", 0))) / 1e9 if heartbeat else None
        return {"target": self.target.name, "watcher": heartbeat, "watcher_age": age, "watcher_state": "UNAVAILABLE" if age is None or age > 15 else heartbeat.get("state"), "probe": {"request_visibility": request_visibility, "result_visibility": result_visibility}, "latency": stats}
