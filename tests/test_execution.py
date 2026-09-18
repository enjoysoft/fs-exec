from __future__ import annotations

import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fs_exec.client import Client
from fs_exec.config import Policy, Target
from fs_exec.policy import PolicyError, checked_environment, command_for
from fs_exec.protocol import TargetPaths, read_final
from fs_exec.util import write_exclusive
from fs_exec.watcher import Watcher


class ExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.root = self.base / "relay"
        self.cwd = self.base / "work"
        self.cwd.mkdir()
        TargetPaths(self.root).initialize()
        self.client = Client(Target("local", self.root), state_dir=self.base / "state")
        self.policy = Policy(allowed_executables=frozenset({sys.executable}), cwd_roots=(self.cwd,), environment_allowlist=frozenset({"DEMO"}), max_runtime_seconds=10)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def process(self, job_id: str) -> dict[str, object]:
        watcher = Watcher(self.root, self.policy, poll_interval=0.01, watcher_id="test-watcher")
        self.assertTrue(watcher.process_job(job_id))
        return read_final(TargetPaths(self.root).result(job_id))

    def test_stdout_stderr_exit_stdin_and_environment_propagate(self) -> None:
        code = "import os,sys; d=sys.stdin.read(); print(os.environ['DEMO']+d); print('problem', file=sys.stderr); raise SystemExit(7)"
        job = self.client.submit(argv=[sys.executable, "-c", code], cwd=str(self.cwd), env={"DEMO": "hello-"}, stdin="input", command_timeout=2)
        result = self.process(job)
        out = next((self.root / "results" / job / "stdout").glob("*.chunk")).read_bytes()
        err = next((self.root / "results" / job / "stderr").glob("*.chunk")).read_text()
        self.assertEqual(result["exit_code"], 7)
        self.assertEqual(out.decode().strip(), "hello-input")
        self.assertIn("problem", err)

    def test_command_timeout_starts_after_claim_not_submission(self) -> None:
        job = self.client.submit(argv=[sys.executable, "-c", "print('late but valid')"], cwd=str(self.cwd), command_timeout=0.2)
        time.sleep(0.3)
        result = self.process(job)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["exit_code"], 0)

    def test_command_timeout_terminates_running_process(self) -> None:
        job = self.client.submit(argv=[sys.executable, "-c", "import time; time.sleep(30)"], cwd=str(self.cwd), command_timeout=0.1)
        result = self.process(job)
        self.assertEqual(result["status"], "TIMED_OUT")

    def test_client_timeout_is_ambiguous_and_retains_job_id(self) -> None:
        job = self.client.submit(argv=[sys.executable, "-c", "print('eventually')"], cwd=str(self.cwd))
        status = self.client.wait(job, 0.02)
        self.assertEqual(status.job_id, job)
        self.assertIsNone(status.result)
        self.assertTrue(TargetPaths(self.root).ready(job).exists())

    def test_input_file_is_materialized_and_piped_to_stdin(self) -> None:
        input_file = self.base / "input.txt"
        input_file.write_text("file-input")
        job = self.client.submit(argv=[sys.executable, "-c", "import sys; print(sys.stdin.read())"], cwd=str(self.cwd), stdin_file=input_file)
        self.process(job)
        output = next((self.root / "results" / job / "stdout").glob("*.chunk")).read_text()
        self.assertEqual(output.strip(), "file-input")

    def test_prestart_cancellation(self) -> None:
        marker = self.cwd / "must-not-exist"
        job = self.client.submit(argv=[sys.executable, "-c", f"open({str(marker)!r}, 'w').close()"], cwd=str(self.cwd))
        self.client.cancel(job)
        self.client.cancel(job)
        result = self.process(job)
        self.assertEqual(result["status"], "CANCELLED")
        self.assertFalse(marker.exists())

    def test_running_cancellation(self) -> None:
        job = self.client.submit(argv=[sys.executable, "-c", "import time; print('started', flush=True); time.sleep(30)"], cwd=str(self.cwd))
        watcher = Watcher(self.root, self.policy, poll_interval=0.01)
        thread = threading.Thread(target=watcher.process_job, args=(job,))
        thread.start()
        self.assertTrue(self.client.wait_for_phase(job, "start", 2))
        self.client.cancel(job)
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(read_final(TargetPaths(self.root).result(job))["status"], "CANCELLED")

    def test_duplicate_observation_executes_once(self) -> None:
        counter = self.cwd / "counter"
        code = f"from pathlib import Path; p=Path({str(counter)!r}); p.write_text(str(int(p.read_text() if p.exists() else 0)+1))"
        job = self.client.submit(argv=[sys.executable, "-c", code], cwd=str(self.cwd))
        watcher = Watcher(self.root, self.policy)
        self.assertTrue(watcher.process_job(job))
        self.assertFalse(watcher.process_job(job))
        self.assertEqual(counter.read_text(), "1")

    def test_restart_recovery_never_replays_started_claim(self) -> None:
        paths = TargetPaths(self.root)
        paths.initialize()
        claim = paths.claims / "j-interrupted"
        claim.mkdir()
        write_exclusive(claim / "CLAIM", b"{}")
        write_exclusive(claim / "STARTED", b"{}")
        Watcher(self.root, self.policy).recover_interrupted()
        result = read_final(paths.result("j-interrupted"))
        self.assertEqual(result["status"], "AVAILABILITY_UNKNOWN")

    def test_restart_preserves_claim_even_when_started_is_not_visible(self) -> None:
        paths = TargetPaths(self.root)
        paths.initialize()
        claim = paths.claims / "j-not-started"
        claim.mkdir()
        write_exclusive(claim / "CLAIM", b"{}")
        Watcher(self.root, self.policy).recover_interrupted()
        self.assertTrue(claim.exists())
        self.assertEqual(read_final(paths.result("j-not-started"))["status"], "AVAILABILITY_UNKNOWN")

    def test_restart_does_not_steal_even_dead_watcher_lock(self) -> None:
        paths = TargetPaths(self.root)
        paths.initialize()
        lock = paths.health / "watcher.lock"
        lock.mkdir()
        write_exclusive(lock / "owner.json", f'{{"host":"{socket.gethostname()}","pid":2147483647,"watcher_id":"dead"}}'.encode())
        watcher = Watcher(self.root, self.policy, watcher_id="replacement")
        with self.assertRaises(RuntimeError):
            watcher.acquire_singleton()
        watcher.release_singleton()
        self.assertTrue(lock.exists())

    def test_artifact_roundtrip_and_checksum(self) -> None:
        output = self.cwd / "report.txt"
        job = self.client.submit(argv=[sys.executable, "-c", f"open({str(output)!r},'w').write('report')"], cwd=str(self.cwd), artifacts=["*.txt"])
        self.process(job)
        destination = self.base / "downloads"
        copied = self.client.download_artifacts(job, destination)
        self.assertEqual(copied, [destination / "report.txt"])
        self.assertEqual(copied[0].read_text(), "report")

    def test_cross_platform_shell_abstraction_and_default_argv(self) -> None:
        from unittest.mock import patch
        policy = Policy(allowed_runtimes=frozenset({"bash", "powershell"}))
        with patch("fs_exec.policy.executable_for", side_effect=lambda value, policy: "/trusted/" + value):
            self.assertEqual(command_for({"argv": ["echo", "$HOME"]}, "linux", policy), ["/trusted/echo", "$HOME"])
            self.assertEqual(command_for({"mode": "shell", "runtime": "bash", "script": "echo ok"}, "linux", policy), ["/trusted/bash", "--noprofile", "--norc", "-euo", "pipefail", "-c", "echo ok"])
            self.assertEqual(command_for({"mode": "shell", "runtime": "powershell", "script": "Write-Output ok"}, "windows", policy), ["/trusted/powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", "Write-Output ok"])

    def test_credentials_are_not_inherited_or_accepted(self) -> None:
        import os
        old = os.environ.get("EXAMPLE_SECRET_TOKEN")
        os.environ["EXAMPLE_SECRET_TOKEN"] = "should-not-leak"
        try:
            self.assertNotIn("EXAMPLE_SECRET_TOKEN", checked_environment({}, Policy()))
            with self.assertRaises(PolicyError):
                checked_environment({"API_TOKEN": "x"}, Policy())
        finally:
            if old is None:
                os.environ.pop("EXAMPLE_SECRET_TOKEN", None)
            else:
                os.environ["EXAMPLE_SECRET_TOKEN"] = old


if __name__ == "__main__":
    unittest.main()
