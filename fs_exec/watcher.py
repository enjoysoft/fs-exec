from __future__ import annotations

import math
import os
import queue
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, BinaryIO

from .config import Policy
from .policy import PolicyError, checked_cwd, checked_environment, command_for
from .protocol import ProtocolError, TargetPaths, publish_final, verify_request
from .util import append_jsonl, atomic_write, canonical_json, now_ns, read_json, safe_name, sha256_file, write_exclusive


class Watcher:
    def __init__(
        self,
        root: Path,
        policy: Policy,
        *,
        platform: str | None = None,
        poll_interval: float = 0.2,
        watcher_id: str | None = None,
        journal: Path | None = None,
    ) -> None:
        self.paths = TargetPaths(root)
        self.policy = policy
        self.platform = platform or ("windows" if os.name == "nt" else "linux")
        self.poll_interval = poll_interval
        self.watcher_id = watcher_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
        self.journal = journal
        self.stop_event = threading.Event()
        self.active: dict[str, subprocess.Popen[bytes]] = {}
        self._lock_path = self.paths.health / "watcher.lock"

    def _journal(self, event: str, **fields: Any) -> None:
        if not self.journal:
            return
        append_jsonl(self.journal, {"at_ns": now_ns(), "event": event, **fields})

    def heartbeat(self, state: str = "READY") -> None:
        atomic_write(self.paths.health / "heartbeat.json", canonical_json({
            "watcher_id": self.watcher_id,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "platform": self.platform,
            "state": state,
            "active_jobs": sorted(self.active),
            "at_ns": now_ns(),
        }))

    def acquire_singleton(self) -> None:
        self.paths.initialize()
        try:
            self._lock_path.mkdir()
            write_exclusive(self._lock_path / "owner.json", canonical_json({"watcher_id": self.watcher_id, "host": socket.gethostname(), "pid": os.getpid()}))
        except FileExistsError:
            try:
                owner = read_json(self._lock_path / "owner.json")
                same_host = owner.get("host") == socket.gethostname()
                pid = int(owner.get("pid", 0))
                alive = pid > 0
                if alive:
                    alive = self._pid_alive(pid)
                if same_host and not alive:
                    shutil.rmtree(self._lock_path)
                    self._lock_path.mkdir()
                    write_exclusive(self._lock_path / "owner.json", canonical_json({"watcher_id": self.watcher_id, "host": socket.gethostname(), "pid": os.getpid()}))
                    return
            except (OSError, ValueError):
                pass
            raise RuntimeError(f"watcher lock exists at {self._lock_path}; verify the old watcher is stopped before removing it")

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if os.name == "nt":
            try:
                completed = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                    capture_output=True, text=True, timeout=5, check=False,
                )
                return completed.returncode != 0 or f'"{pid}"' in completed.stdout
            except (OSError, subprocess.TimeoutExpired):
                return True
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except (PermissionError, OSError):
            return True

    def release_singleton(self) -> None:
        try:
            owner = read_json(self._lock_path / "owner.json")
            if owner.get("watcher_id") == self.watcher_id:
                shutil.rmtree(self._lock_path)
        except (OSError, ValueError):
            pass

    def _claim(self, job_id: str) -> Path | None:
        claim = self.paths.claims / safe_name(job_id)
        try:
            claim.mkdir()
            write_exclusive(claim / "CLAIM", canonical_json({"watcher_id": self.watcher_id, "fence": uuid.uuid4().hex, "claimed_ns": now_ns()}))
            return claim
        except FileExistsError:
            return None

    def _reader(self, stream: BinaryIO, result_dir: Path, name: str, events: queue.Queue[tuple[str, bytes]], chunk_size: int = 64 * 1024) -> None:
        try:
            while True:
                data = stream.read(chunk_size)
                if not data:
                    break
                events.put((name, data))
        finally:
            stream.close()

    def _terminate_tree(self, process: subprocess.Popen[bytes]) -> None:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            return
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            process.terminate()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                process.kill()

    def _approve(self, manifest: dict[str, Any]) -> None:
        hook = self.policy.approval_hook
        if hook is None:
            return
        if not hook.is_file():
            raise PolicyError(f"approval hook does not exist: {hook}")
        completed = subprocess.run(
            [str(hook)], input=canonical_json(manifest), stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE, shell=False, timeout=30,
        )
        if completed.returncode:
            reason = completed.stderr.decode("utf-8", "replace")[:1000].strip()
            raise PolicyError(f"approval hook rejected request: {reason or completed.returncode}")

    def _copy_artifacts(self, cwd: Path, patterns: list[str], result_dir: Path) -> tuple[list[dict[str, Any]], bool]:
        entries: list[dict[str, Any]] = []
        total = 0
        truncated = False
        seen: set[Path] = set()
        for pattern in patterns:
            pattern_path = Path(pattern)
            if pattern_path.is_absolute() or ".." in pattern_path.parts or "**" in pattern_path.parts:
                raise PolicyError(f"artifact pattern must be a bounded cwd-relative glob: {pattern}")
            for source in cwd.glob(pattern):
                source = source.resolve()
                if source in seen or not source.is_file() or not (source == cwd or cwd in source.parents):
                    continue
                seen.add(source)
                size = source.stat().st_size
                if total + size > self.policy.max_artifact_bytes:
                    truncated = True
                    continue
                relative = source.relative_to(cwd)
                name = str(relative).replace("\\", "/")
                destination = result_dir / "artifacts" / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
                entries.append({"name": name, "size": size, "sha256": sha256_file(destination)})
                total += size
        return entries, truncated

    def process_job(self, job_id: str) -> bool:
        result_dir = self.paths.result(job_id)
        if (result_dir / "FINAL").exists():
            return False
        claim = self._claim(job_id)
        if claim is None:
            return False
        result_dir.mkdir(parents=True, exist_ok=True)
        write_exclusive(result_dir / "CLAIMED", canonical_json({"watcher_id": self.watcher_id, "at_ns": now_ns()}))
        self._journal("claimed", job_id=job_id)
        request_dir = self.paths.ready(job_id)
        started_ns: int | None = None
        local_upload_dir: Path | None = None
        try:
            manifest = verify_request(request_dir)
            if (request_dir / "request.json").stat().st_size > self.policy.max_request_bytes:
                raise PolicyError("request manifest exceeds policy limit")
            if now_ns() > int(manifest["expires_ns"]):
                publish_final(result_dir, self._result(job_id, "EXPIRED", None, "request expired before execution"))
                return True
            upload_bytes = sum(int(item["size"]) for item in manifest.get("uploads", []))
            if upload_bytes > self.policy.max_upload_bytes:
                raise PolicyError("uploaded data exceeds policy limit")
            if (self.paths.control / "cancel" / job_id).exists():
                publish_final(result_dir, self._result(job_id, "CANCELLED", None, "cancelled before execution"))
                return True
            local_upload_dir = Path(tempfile.mkdtemp(prefix=f"fs-exec-{job_id}-"))
            for upload in manifest.get("uploads", []):
                name = safe_name(str(upload["name"]))
                local_copy = local_upload_dir / name
                shutil.copyfile(request_dir / "uploads" / name, local_copy)
                if local_copy.stat().st_size != int(upload["size"]) or sha256_file(local_copy) != upload["sha256"]:
                    raise ProtocolError(f"upload changed while materializing: {name}")
            command = command_for(manifest, self.platform, self.policy)
            cwd = checked_cwd(manifest.get("cwd"), self.policy)
            environment = checked_environment(dict(manifest.get("env", {})), self.policy)
            environment["FS_EXEC_UPLOAD_DIR"] = str(local_upload_dir)
            self._approve(manifest)
            timeout = min(float(manifest.get("command_timeout", self.policy.max_runtime_seconds)), self.policy.max_runtime_seconds)
            if not math.isfinite(timeout) or timeout <= 0:
                raise PolicyError("command timeout must be a positive finite number")
            stdin: BinaryIO | int
            stdin_path = manifest.get("stdin_upload")
            stdin_file = None
            if stdin_path:
                stdin_file = (local_upload_dir / safe_name(str(stdin_path))).open("rb")
                stdin = stdin_file
            elif "stdin" in manifest:
                stdin = subprocess.PIPE
            else:
                stdin = subprocess.DEVNULL
            started_ns = now_ns()
            write_exclusive(claim / "STARTED", canonical_json({"started_ns": started_ns}))
            write_exclusive(result_dir / "RUNNING", canonical_json({"started_ns": started_ns}))
            self._journal("started", job_id=job_id)
            popen_options: dict[str, Any] = {"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            process = subprocess.Popen(command, cwd=cwd, env=environment, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, **popen_options)
            self.active[job_id] = process
            if "stdin" in manifest and process.stdin:
                process.stdin.write(str(manifest["stdin"]).encode())
                process.stdin.close()
            events: queue.Queue[tuple[str, bytes]] = queue.Queue(maxsize=16)
            threads = [
                threading.Thread(target=self._reader, args=(process.stdout, result_dir, "stdout", events), daemon=True),  # type: ignore[arg-type]
                threading.Thread(target=self._reader, args=(process.stderr, result_dir, "stderr", events), daemon=True),  # type: ignore[arg-type]
            ]
            for thread in threads:
                thread.start()
            counts = {"stdout": 0, "stderr": 0}
            bytes_written = 0
            output_truncated = False
            cancelled = False
            timed_out = False
            deadline = time.monotonic() + timeout
            next_heartbeat = time.monotonic()
            while process.poll() is None or any(thread.is_alive() for thread in threads) or not events.empty():
                try:
                    stream_name, original = events.get(timeout=0.03)
                    allowed = max(0, self.policy.max_output_bytes - bytes_written)
                    if allowed:
                        data = original[:allowed]
                        index = counts[stream_name]
                        atomic_write(result_dir / stream_name / f"{index:08d}.chunk", data)
                        counts[stream_name] += 1
                        bytes_written += len(data)
                    if len(original) > allowed:
                        output_truncated = True
                except queue.Empty:
                    pass
                if process.poll() is None and (self.paths.control / "cancel" / job_id).exists():
                    cancelled = True
                    self._terminate_tree(process)
                if process.poll() is None and time.monotonic() >= deadline:
                    timed_out = True
                    self._terminate_tree(process)
                if time.monotonic() >= next_heartbeat:
                    self.heartbeat("BUSY")
                    next_heartbeat = time.monotonic() + 2
            for thread in threads:
                thread.join(timeout=1)
            if stdin_file:
                stdin_file.close()
            exit_code = process.wait()
            artifacts, artifact_truncated = self._copy_artifacts(cwd, list(map(str, manifest.get("artifacts", []))), result_dir)
            status = "CANCELLED" if cancelled else "TIMED_OUT" if timed_out else "COMPLETED"
            result = self._result(job_id, status, exit_code, None, started_ns=started_ns)
            result.update({"stdout_chunks": counts["stdout"], "stderr_chunks": counts["stderr"], "output_truncated": output_truncated, "artifacts": artifacts, "artifacts_truncated": artifact_truncated})
            publish_final(result_dir, result)
            self._journal("final", job_id=job_id, status=status, exit_code=exit_code)
            return True
        except Exception as exc:
            publish_final(result_dir, self._result(job_id, "REJECTED", None, str(exc), started_ns=started_ns))
            self._journal("rejected", job_id=job_id, error=str(exc))
            return True
        finally:
            self.active.pop(job_id, None)
            if local_upload_dir is not None:
                shutil.rmtree(local_upload_dir, ignore_errors=True)

    def _result(self, job_id: str, status: str, exit_code: int | None, error: str | None, *, started_ns: int | None = None) -> dict[str, Any]:
        return {"protocol": 1, "job_id": job_id, "watcher_id": self.watcher_id, "status": status, "exit_code": exit_code, "error": error, "started_ns": started_ns, "finished_ns": now_ns(), "stdout_chunks": 0, "stderr_chunks": 0, "artifacts": []}

    def recover_interrupted(self) -> None:
        """Finalize started claims rather than risk replaying a command after restart."""
        for claim in self.paths.claims.iterdir():
            if not claim.is_dir():
                continue
            job_id = claim.name
            result_dir = self.paths.result(job_id)
            if (result_dir / "FINAL").exists():
                continue
            if (claim / "STARTED").exists():
                publish_final(result_dir, self._result(job_id, "AVAILABILITY_UNKNOWN", None, "watcher restarted after command start; command was not replayed"))
            else:
                # STARTED is written before Popen, so an unstarted claim can be
                # released after singleton ownership has transferred.
                shutil.rmtree(claim)

    def process_pings(self) -> None:
        for ping in self.paths.health.joinpath("pings").glob("*.ready"):
            probe_id = ping.stem
            pong = self.paths.health / "pongs" / probe_id
            if pong.exists():
                continue
            try:
                value = read_json(ping / "PING")
                observed_ns = now_ns()
                write_exclusive(pong, canonical_json({"probe_id": probe_id, "client_ns": value["client_ns"], "observed_ns": observed_ns, "responded_ns": now_ns(), "watcher_id": self.watcher_id}))
            except (OSError, ValueError, KeyError):
                continue

    def run(self, *, once: bool = False) -> None:
        self.acquire_singleton()
        try:
            self.recover_interrupted()
            while not self.stop_event.is_set():
                self.heartbeat("READY")
                self.process_pings()
                for request in self.paths.inbox.glob("*.ready"):
                    if (request / "COMMIT").exists():
                        self.process_job(request.name.removesuffix(".ready"))
                if once:
                    return
                self.stop_event.wait(self.poll_interval)
        finally:
            self.heartbeat("STOPPED")
            self.release_singleton()
