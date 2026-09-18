from __future__ import annotations

import json
import math
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO

from .config import Target
from .latency import LatencyStore
from .protocol import (
    ProtocolError,
    TargetPaths,
    new_job_id,
    publish_cancel,
    publish_request,
    read_final,
)
from .util import (
    canonical_json,
    check_path,
    copy_bounded,
    exact_wait,
    fsync_directory,
    now_ns,
    parent_fd,
    read_bounded,
    read_json,
    safe_name,
    sha256_bytes,
    write_exclusive,
)


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
        try:
            if (result_dir / "FINAL").exists():
                result = read_final(result_dir)
                state = "COMPLETED" if result.get("status") == "COMPLETED" else str(result.get("status"))
                return JobStatus(job_id, state, watcher_state, result)
            if (result_dir / "result.json").exists() or any(result_dir.glob("result-*.json")):
                return JobStatus(job_id, "RESULT_PROPAGATING", watcher_state)
            if (result_dir / "RUNNING").exists():
                return JobStatus(job_id, "AVAILABILITY_UNKNOWN" if heartbeat_stale else "RUNNING", watcher_state)
            if (result_dir / "CLAIMED").exists():
                return JobStatus(job_id, "AVAILABILITY_UNKNOWN" if heartbeat_stale else "CLAIMED", watcher_state)
            if self.paths.ready(job_id).joinpath("COMMIT").exists():
                return JobStatus(job_id, "AVAILABILITY_UNKNOWN" if heartbeat_stale else "VISIBLE_UNCLAIMED", watcher_state)
        except (OSError, ValueError, ProtocolError, KeyError, TypeError):
            return JobStatus(job_id, "RESULT_PROPAGATING", watcher_state)
        return JobStatus(job_id, "AWAITING_VISIBILITY", watcher_state)

    def wait(self, job_id: str, total_timeout: float, *, stream: bool = False, stdout: BinaryIO | None = None, stderr: BinaryIO | None = None, result_timeout: float | None = None) -> JobStatus:
        deadline = time.monotonic() + total_timeout
        propagation_deadline: float | None = None
        stdout = stdout or sys.stdout.buffer
        stderr = stderr or sys.stderr.buffer
        counts = {"stdout": 0, "stderr": 0}
        observed: dict[str, list[dict[str, Any]]] = {"stdout": [], "stderr": []}
        while True:
            status = self.status(job_id)
            if stream and status.result is None and status.state != "RESULT_PROPAGATING":
                for name, output in (("stdout", stdout), ("stderr", stderr)):
                    # Live output is provisional. Bound work per iteration so
                    # an active producer cannot starve timeout/FINAL checks.
                    for _ in range(16):
                        chunk = self.paths.result(job_id) / name / f"{counts[name]:08d}.chunk"
                        if counts[name] >= 4096:
                            break
                        try:
                            data = read_bounded(chunk, 65536)
                        except (OSError, ValueError):
                            break
                        output.write(data)
                        output.flush()
                        observed[name].append({"size": len(data), "sha256": sha256_bytes(data)})
                        counts[name] += 1
            if status.result is not None:
                if propagation_deadline is None:
                    propagation_deadline = time.monotonic() + result_timeout if result_timeout is not None else deadline
                final_propagation_deadline = min(deadline, propagation_deadline)
                for name, output in (("stdout", stdout), ("stderr", stderr)):
                    entries = status.result.get("streams", {}).get(name, [])
                    if observed[name] != entries[:counts[name]]:
                        raise ProtocolError("provisional output failed FINAL integrity verification")
                    while counts[name] < len(entries):
                        chunk = self.paths.result(job_id) / name / f"{counts[name]:08d}.chunk"
                        entry = entries[counts[name]]
                        try:
                            data = read_bounded(chunk, 65536)
                            if len(data) != entry["size"] or sha256_bytes(data) != entry["sha256"]:
                                raise ProtocolError("chunk checksum mismatch")
                        except (OSError, ValueError, ProtocolError):
                            if time.monotonic() >= final_propagation_deadline:
                                return JobStatus(job_id, "RESULT_PROPAGATING", status.watcher_state)
                            time.sleep(min(0.03, max(0, final_propagation_deadline - time.monotonic())))
                            continue
                        if time.monotonic() > final_propagation_deadline:
                            return JobStatus(job_id, "RESULT_PROPAGATING", status.watcher_state)
                        if stream:
                            output.write(data)
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
        deadline = time.monotonic() + visibility_timeout
        status = self.wait(job_id, visibility_timeout)
        if status.result is None:
            raise ProtocolError("final result is still propagating")
        result = status.result
        copied: list[Path] = []
        for artifact in result.get("artifacts", []):
            relative = Path(str(artifact["name"]))
            if relative.is_absolute() or not relative.parts or "\\" in str(relative):
                raise ValueError(f"unsafe artifact path: {relative}")
            for part in relative.parts:
                safe_name(part)
            source = self.paths.result(job_id) / "artifacts" / relative
            output = destination / relative
            check_path(output)
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.tmp")
            try:
                while True:
                    try:
                        if time.monotonic() > deadline:
                            raise ProtocolError("artifact propagation timeout")
                        size, digest = copy_bounded(source, temporary, int(artifact["size"]))
                        if size != artifact["size"] or digest != artifact["sha256"]:
                            raise ProtocolError("artifact checksum mismatch")
                        break
                    except (OSError, ValueError, ProtocolError):
                        temporary.unlink(missing_ok=True)
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(min(0.03, max(0, deadline - time.monotonic())))
                with parent_fd(output) as directory:
                    os.link(temporary if directory is None else temporary.name,
                            output if directory is None else output.name,
                            src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
                fsync_directory(output.parent)
            finally:
                temporary.unlink(missing_ok=True)
            copied.append(output)
        return copied

    def _heartbeat(self) -> dict[str, Any] | None:
        try:
            heartbeat = read_json(self.paths.health / "heartbeat.json")
            int(heartbeat["at_ns"])
            return heartbeat
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def relay_health(self) -> dict[str, Any] | None:
        try:
            # Keep fs-exec independent of the sidecar implementation.
            relay = json.loads(read_bounded(self.paths.health / "rally.json", 65536))
            age = (now_ns() - int(relay["at_ns"])) / 1e9
            overhead = float(relay["recommended_overhead"])
            if not math.isfinite(overhead) or overhead <= 0:
                return None
            return {**relay, "age_seconds": age, "stale": not 0 <= age <= 15}
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def transport_overhead(self) -> float:
        if self.target.transport_timeout is not None:
            return self.target.transport_timeout
        stats = self.latency.stats()
        # Client probes already traverse both relay legs. Never add relay RTT
        # to fresh end-to-end measurements. Relay advice is a fallback floor.
        relay = self.relay_health() if stats.stale else None
        return max(stats.recommended_overhead, float(relay["recommended_overhead"])) if relay and not relay["stale"] else stats.recommended_overhead

    def health(self, *, probe: bool = True, timeout: float = 30) -> dict[str, Any]:
        self.paths.require_client_namespace()
        heartbeat = self._heartbeat()
        request_visibility = result_visibility = round_trip = None
        if probe:
            probe_id = f"p-{uuid.uuid4().hex}"
            staging = self.paths.health / "pings" / f"{probe_id}.staging"
            ready = self.paths.health / "pings" / f"{probe_id}.ready"
            staging.mkdir(mode=0o770)
            sent_ns = now_ns()
            sent_monotonic = time.monotonic()
            write_exclusive(staging / "PING", canonical_json({"probe_id": probe_id, "client_ns": sent_ns}))
            os.replace(staging, ready)
            fsync_directory(ready.parent)
            pong_path = self.paths.health / "pongs" / probe_id
            deadline = time.monotonic() + timeout
            while True:
                try:
                    pong = read_json(pong_path)
                    if pong["probe_id"] != probe_id or pong["client_ns"] != sent_ns:
                        raise ValueError("probe identity mismatch")
                    received_ns = now_ns()
                    request_visibility = max(0.0, (int(pong["observed_ns"]) - sent_ns) / 1e9)
                    result_visibility = max(0.0, (received_ns - int(pong.get("responded_ns", pong["observed_ns"]))) / 1e9)
                    round_trip = time.monotonic() - sent_monotonic
                    self.latency.record(request_visibility, result_visibility, round_trip=round_trip)
                    break
                except (OSError, ValueError, KeyError, TypeError):
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(0.03)
            heartbeat = self._heartbeat()
        stats = asdict(self.latency.stats())
        age = (now_ns() - int(heartbeat.get("at_ns", 0))) / 1e9 if heartbeat else None
        return {"target": self.target.name, "watcher": heartbeat, "watcher_age": age, "watcher_state": "UNAVAILABLE" if age is None or age > 15 else heartbeat.get("state"), "probe": {"succeeded": request_visibility is not None if probe else None, "request_visibility": request_visibility, "result_visibility": result_visibility, "round_trip": round_trip}, "latency": stats, "relay": self.relay_health(), "transport_overhead": self.transport_overhead()}
