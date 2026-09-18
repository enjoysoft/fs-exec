from __future__ import annotations

import math
import os
import queue
import select
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
from .protocol import (
    PROTOCOL_VERSION,
    ProtocolError,
    TargetPaths,
    publish_final,
    verify_request,
)
from .util import (
    append_jsonl,
    atomic_write,
    canonical_json,
    check_path,
    copy_bounded,
    fsync_directory,
    now_ns,
    read_json,
    safe_name,
    sha256_bytes,
    write_exclusive,
)


class PrelaunchStop(RuntimeError):
    pass


class CleanupFailed(RuntimeError):
    """No terminal result is safe when process termination could not finish."""


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
        self._singleton = False
        self._fences: dict[str, str] = {}
        self._heartbeat_failed = threading.Event()
        self._cleanup_failed = False

    def _journal(self, event: str, **fields: Any) -> None:
        if not self.journal:
            return
        try:
            append_jsonl(self.journal, {"at_ns": now_ns(), "event": event, **fields})
        except OSError:
            pass  # The optional audit sink cannot interrupt process cleanup.

    def heartbeat(self, state: str = "READY") -> None:
        if self._singleton:
            self._check_singleton()
        atomic_write(self.paths.health / "heartbeat.json", canonical_json({
            "watcher_id": self.watcher_id,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "platform": self.platform,
            "state": state,
            "active_jobs": sorted(self._fences),
            "at_ns": now_ns(),
        }))

    def acquire_singleton(self) -> None:
        self.paths.initialize()
        try:
            self._lock_path.mkdir(mode=0o770)
            write_exclusive(self._lock_path / "owner.json", canonical_json({"watcher_id": self.watcher_id, "host": socket.gethostname(), "pid": os.getpid()}))
            fsync_directory(self.paths.health)
            self._singleton = True
        except FileExistsError:
            raise RuntimeError(f"watcher lock exists at {self._lock_path}; verify the old watcher is stopped before removing it")

    def release_singleton(self) -> None:
        if self._cleanup_failed:
            return  # Operator must stop any surviving processes before takeover.
        try:
            owner = read_json(self._lock_path / "owner.json")
            if owner.get("watcher_id") == self.watcher_id:
                shutil.rmtree(self._lock_path)
        except (OSError, ValueError):
            pass
        self._singleton = False

    def _check_singleton(self) -> None:
        if read_json(self._lock_path / "owner.json").get("watcher_id") != self.watcher_id:
            raise ProtocolError("watcher ownership lost")

    def _check_owner(self, job_id: str) -> None:
        if self._singleton:
            self._check_singleton()
        claim = read_json(self.paths.claims / job_id / "CLAIM")
        if claim.get("fence") != self._fences[job_id] or claim.get("watcher_id") != self.watcher_id:
            raise ProtocolError("job ownership lost")
        if (self.paths.result(job_id) / "FINAL").exists():
            raise ProtocolError("job already finalized")

    def _prelaunch(self, manifest: dict[str, Any]) -> None:
        job_id = manifest["job_id"]
        self._check_owner(job_id)
        if self._heartbeat_failed.is_set():
            raise ProtocolError("heartbeat publication failed")
        if self.stop_event.is_set() or (self.paths.control / "cancel" / job_id).exists():
            raise PrelaunchStop("CANCELLED")
        current = now_ns()
        published = int(manifest["published_ns"])
        if published > current + 5_000_000_000:
            raise PolicyError("request timestamp is in the future")
        published = min(published, int(read_json(self.paths.claims / job_id / "CLAIM")["claimed_ns"]))
        if current >= min(int(manifest["expires_ns"]), published + int(self.policy.max_queue_age_seconds * 1e9)):
            raise PrelaunchStop("EXPIRED")

    def _claim(self, job_id: str) -> Path | None:
        claim = self.paths.claims / safe_name(job_id)
        check_path(claim)
        try:
            claim.mkdir(mode=0o770)
            fsync_directory(self.paths.claims)
            fence = uuid.uuid4().hex
            write_exclusive(claim / "CLAIM", canonical_json({"job_id": job_id, "watcher_id": self.watcher_id, "fence": fence, "claimed_ns": now_ns()}))
            self._fences[job_id] = fence
            return claim
        except FileExistsError:
            return None

    def _reader(self, stream: BinaryIO, name: str, events: queue.Queue, stop: threading.Event, chunk_size: int = 64 * 1024) -> None:
        try:
            while not stop.is_set():
                if os.name == "posix":
                    if not select.select([stream], [], [], 0.05)[0]:
                        continue
                    data = os.read(stream.fileno(), chunk_size)
                else:
                    data = stream.read1(chunk_size)
                if not data:
                    break
                while not stop.is_set():
                    try:
                        events.put((name, data), timeout=0.05)
                        break
                    except queue.Full:
                        continue
        except Exception:
            stop.set()
        finally:
            stream.close()

    def _terminate_tree(self, process: subprocess.Popen[bytes]) -> None:
        try:
            self._kill_and_reap(process)
        except Exception as exc:
            self._cleanup_failed = True
            self.stop_event.set()
            raise CleanupFailed("process cleanup failed; operator intervention required") from exc

    def _kill_and_reap(self, process: subprocess.Popen[bytes]) -> None:
        if os.name == "nt":
            try:
                subprocess.run([str(Path(os.environ["SystemRoot"]) / "System32" / "taskkill.exe"), "/PID", str(process.pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=5)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
        else:
            # The session leader may already have exited. Its original pid is
            # still the process-group id; getpgid(pid) would lose descendants.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                process.kill()
            process.wait()

    def _approve(self, manifest: dict[str, Any]) -> None:
        hook = self.policy.approval_hook
        if hook is None:
            return
        if not hook.is_file():
            raise PolicyError(f"approval hook does not exist: {hook}")
        with tempfile.TemporaryFile() as stdin:
            stdin.write(canonical_json(manifest))
            stdin.seek(0)
            process = subprocess.Popen([str(hook)], stdin=stdin, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, shell=False, **self._popen_options())
            try:
                deadline = time.monotonic() + 30
                while process.poll() is None:
                    self._prelaunch(manifest)
                    if time.monotonic() >= deadline:
                        raise PolicyError("approval hook timed out")
                    time.sleep(0.03)
                if process.returncode:
                    raise PolicyError("approval hook rejected request")
            finally:
                self._terminate_tree(process)

    @staticmethod
    def _popen_options() -> dict[str, Any]:
        return {"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}

    def _copy_artifacts(self, cwd: Path, patterns: list[str], result_dir: Path) -> tuple[list[dict[str, Any]], bool]:
        entries: list[dict[str, Any]] = []
        total = 0
        truncated = False
        seen: set[Path] = set()
        if len(patterns) > self.policy.max_artifact_files:
            raise PolicyError("too many artifact patterns")
        for pattern in patterns:
            pattern_path = Path(pattern)
            if pattern_path.is_absolute() or ".." in pattern_path.parts or "**" in pattern or ":" in pattern or "\\" in pattern:
                raise PolicyError(f"artifact pattern must be a bounded cwd-relative glob: {pattern}")
            for source in cwd.glob(pattern):
                check_path(source)
                if source in seen or not source.is_file() or not (source == cwd or cwd in source.parents):
                    continue
                if len(seen) >= self.policy.max_artifact_files:
                    return entries, True
                seen.add(source)
                size = source.stat().st_size
                if total + size > self.policy.max_artifact_bytes:
                    truncated = True
                    continue
                relative = source.relative_to(cwd)
                for part in relative.parts:
                    safe_name(part)
                name = str(relative).replace("\\", "/")
                destination = result_dir / "artifacts" / relative
                self._check_owner(result_dir.name)
                try:
                    size, digest = copy_bounded(source, destination, self.policy.max_artifact_bytes - total, shared=True)
                except ValueError:
                    truncated = True
                    continue
                entries.append({"name": name, "size": size, "sha256": digest})
                total += size
        return entries, truncated

    def process_job(self, job_id: str) -> bool:
        claim = None
        local_upload_dir: Path | None = None
        pulse_stop = threading.Event()
        pulse = None
        try:
            result_dir = self.paths.result(job_id)
            check_path(result_dir)
            if (result_dir / "FINAL").exists():
                return False
            claim = self._claim(job_id)
            if claim is None:
                return False
            self._heartbeat_failed.clear()
            self.heartbeat("BUSY")
            pulse = threading.Thread(target=self._pulse, args=(pulse_stop,), daemon=True)
            pulse.start()
            self._check_owner(job_id)
            write_exclusive(result_dir / "CLAIMED", canonical_json(self._identity(job_id)))
            self._journal("claimed", job_id=job_id)
            request_dir = self.paths.ready(job_id)
            manifest = verify_request(request_dir, policy=self.policy)
            self._prelaunch(manifest)
            local_upload_dir = Path(tempfile.mkdtemp(prefix=f"fs-exec-{job_id}-"))
            for upload in manifest.get("uploads", []):
                self._prelaunch(manifest)
                name = safe_name(str(upload["name"]))
                local_copy = local_upload_dir / name
                size, digest = copy_bounded(request_dir / "uploads" / name, local_copy, upload["size"])
                if size != upload["size"] or digest != upload["sha256"]:
                    raise ProtocolError(f"upload changed while materializing: {name}")
            command = command_for(manifest, self.platform, self.policy)
            cwd = checked_cwd(manifest.get("cwd"), self.policy)
            environment = checked_environment(dict(manifest.get("env", {})), self.policy)
            environment["FS_EXEC_UPLOAD_DIR"] = str(local_upload_dir)
            self._approve(manifest)
            timeout = min(float(manifest.get("command_timeout", self.policy.max_runtime_seconds)), self.policy.max_runtime_seconds)
            if not math.isfinite(timeout) or timeout <= 0:
                raise PolicyError("command timeout must be a positive finite number")
            result = self._execute(manifest, command, cwd, environment, local_upload_dir, timeout)
            artifacts, truncated = self._copy_artifacts(cwd, list(map(str, manifest.get("artifacts", []))), result_dir)
            result.update(artifacts=artifacts, artifacts_truncated=truncated)
            self._check_owner(job_id)
            publish_final(result_dir, result)
            self._journal("final", job_id=job_id, status=result["status"], exit_code=result["exit_code"])
            return True
        except Exception as exc:
            # _execute and _approve have already killed/reaped their children.
            # Do not echo arbitrary exceptions: they can contain request secrets.
            status = str(exc) if isinstance(exc, PrelaunchStop) else "FAILED" if job_id in self.active else "REJECTED"
            self._journal("job_error", job_id=job_id, error_type=type(exc).__name__)
            if isinstance(exc, CleanupFailed):
                return claim is not None  # Retain the claim; never publish false terminality.
            if claim is not None:
                try:
                    self._check_owner(job_id)
                    publish_final(self.paths.result(job_id), self._result(job_id, status, None, "request stopped or rejected; inspect owner policy and filesystem"))
                except Exception as publication_error:
                    self._journal("publication_failed", job_id=job_id, error_type=type(publication_error).__name__)
            return claim is not None
        finally:
            pulse_stop.set()
            if pulse is not None and pulse.ident is not None:
                pulse.join(timeout=3)
            self.active.pop(job_id, None)
            self._fences.pop(job_id, None)
            if local_upload_dir is not None:
                shutil.rmtree(local_upload_dir, ignore_errors=True)

    def _pulse(self, stop: threading.Event) -> None:
        next_heartbeat = time.monotonic() + 1
        while not stop.wait(0.05):
            try:
                if time.monotonic() >= next_heartbeat:
                    self.heartbeat("BUSY")
                    next_heartbeat = time.monotonic() + 1
                self.process_pings()
            except Exception:
                self._heartbeat_failed.set()
                return

    def _execute(self, manifest: dict[str, Any], command: list[str], cwd: Path, environment: dict[str, str], uploads: Path, timeout: float) -> dict[str, Any]:
        job_id = manifest["job_id"]
        result_dir = self.paths.result(job_id)
        process = None
        stdin_file = None
        threads: list[threading.Thread] = []
        reader_stop = threading.Event()
        try:
            stdin: BinaryIO | int = subprocess.DEVNULL
            stdin_path = manifest.get("stdin_upload")
            if stdin_path:
                stdin_file = (uploads / safe_name(str(stdin_path))).open("rb")
                stdin = stdin_file
            elif "stdin" in manifest:
                stdin_file = tempfile.TemporaryFile()
                stdin_file.write(str(manifest["stdin"]).encode())
                stdin_file.seek(0)
                stdin = stdin_file
            self._prelaunch(manifest)
            started_ns = now_ns()
            transition = canonical_json({**self._identity(job_id), "started_ns": started_ns})
            write_exclusive(self.paths.claims / job_id / "STARTED", transition)
            self._prelaunch(manifest)
            write_exclusive(result_dir / "RUNNING", transition)
            self._journal("started", job_id=job_id)
            self._prelaunch(manifest)
            deadline = time.monotonic() + timeout
            process = subprocess.Popen(command, cwd=cwd, env=environment, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, **self._popen_options())
            self.active[job_id] = process
            events: queue.Queue[tuple[str, bytes]] = queue.Queue(maxsize=16)
            for name, pipe in (("stdout", process.stdout), ("stderr", process.stderr)):
                thread = threading.Thread(target=self._reader, args=(pipe, name, events, reader_stop), daemon=True)
                thread.start()
                threads.append(thread)
            chunks: dict[str, list[dict[str, Any]]] = {"stdout": [], "stderr": []}
            bytes_written = 0
            output_truncated = False
            cancelled = False
            timed_out = False
            tree_stopped = False
            drain_deadline = float("inf")
            while process.poll() is None or any(thread.is_alive() for thread in threads) or not events.empty():
                self._check_owner(job_id)
                if reader_stop.is_set() or self._heartbeat_failed.is_set():
                    raise ProtocolError("output reader or heartbeat failed")
                if not tree_stopped:
                    cancelled = self.stop_event.is_set() or (self.paths.control / "cancel" / job_id).exists()
                    timed_out = not cancelled and time.monotonic() >= deadline
                    if cancelled or timed_out:
                        self._terminate_tree(process)
                        tree_stopped = True
                        drain_deadline = time.monotonic() + 2
                try:
                    stream_name, original = events.get(timeout=0.03)
                    allowed = max(0, self.policy.max_output_bytes - bytes_written)
                    if len(chunks[stream_name]) >= 4096:
                        allowed = 0
                    if allowed:
                        data = original[:allowed]
                        index = len(chunks[stream_name])
                        self._check_owner(job_id)
                        write_exclusive(result_dir / stream_name / f"{index:08d}.chunk", data)
                        chunks[stream_name].append({"size": len(data), "sha256": sha256_bytes(data)})
                        bytes_written += len(data)
                    if len(original) > allowed:
                        output_truncated = True
                except queue.Empty:
                    pass
                if time.monotonic() > drain_deadline:
                    raise ProtocolError("descendant retained output pipes")
            if reader_stop.is_set() or self._heartbeat_failed.is_set():
                raise ProtocolError("output reader or heartbeat failed")
            exit_code = process.wait()
            status = "CANCELLED" if cancelled else "TIMED_OUT" if timed_out else "COMPLETED"
            result = self._result(job_id, status, exit_code, None, started_ns=started_ns)
            result.update({"stdout_chunks": len(chunks["stdout"]), "stderr_chunks": len(chunks["stderr"]), "streams": chunks, "output_truncated": output_truncated})
            return result
        finally:
            # This runs for all exceptions, including interrupts and partial
            # thread startup, before any terminal result can be published.
            reader_stop.set()
            try:
                if process is not None:
                    self._terminate_tree(process)
            finally:
                for thread in threads:
                    thread.join(timeout=1)
                # A detached descendant can hold a pipe indefinitely. Do not
                # block on closing a BufferedReader another thread is reading.
                if process is not None and not any(thread.is_alive() for thread in threads):
                    for pipe in (process.stdout, process.stderr):
                        if pipe is not None:
                            pipe.close()
                if stdin_file is not None:
                    stdin_file.close()

    def _identity(self, job_id: str) -> dict[str, Any]:
        return {"job_id": job_id, "watcher_id": self.watcher_id, "fence": self._fences.get(job_id), "at_ns": now_ns()}

    def _result(self, job_id: str, status: str, exit_code: int | None, error: str | None, *, started_ns: int | None = None) -> dict[str, Any]:
        return {"protocol": PROTOCOL_VERSION, **self._identity(job_id), "status": status, "exit_code": exit_code, "error": error, "started_ns": started_ns, "finished_ns": now_ns(), "stdout_chunks": 0, "stderr_chunks": 0, "streams": {"stdout": [], "stderr": []}, "artifacts": []}

    def recover_interrupted(self) -> None:
        """Absence of STARTED is not evidence of absence on a cached share."""
        for claim in self.paths.claims.iterdir():
            if not claim.is_dir():
                continue
            try:
                check_path(claim)
                job_id = safe_name(claim.name)
                result_dir = self.paths.result(job_id)
                if not (result_dir / "FINAL").exists():
                    if self._singleton:
                        self._check_singleton()
                    result = self._result(job_id, "AVAILABILITY_UNKNOWN", None, "pre-existing claim; command was not replayed")
                    result["fence"] = f"recovery-{uuid.uuid4().hex}"
                    publish_final(result_dir, result)
            except Exception as exc:
                self._journal("recovery_failed", job_id=claim.name, error_type=type(exc).__name__)

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
            except (OSError, ValueError, KeyError, TypeError):
                continue

    def run(self, *, once: bool = False) -> None:
        self.acquire_singleton()
        try:
            self.recover_interrupted()
            while not self.stop_event.is_set():
                self.heartbeat("READY")
                self.process_pings()
                for request in self.paths.inbox.glob("*.ready"):
                    if self.stop_event.is_set():
                        break
                    if (request / "COMMIT").exists():
                        self.process_job(request.name.removesuffix(".ready"))
                if once:
                    return
                self.stop_event.wait(self.poll_interval)
        finally:
            try:
                self.heartbeat("STOPPED")
            finally:
                self.release_singleton()
