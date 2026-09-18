from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import errno
import io
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from fs_exec.cli import cmd_health
from fs_exec.client import Client
from fs_exec.config import Policy, Target
from fs_exec.policy import PolicyError, checked_environment, command_for
from fs_exec.protocol import (
    ProtocolError,
    TargetPaths,
    publish_final,
    read_final,
    verify_request,
)
from fs_exec.util import (
    canonical_json,
    copy_bounded,
    fsync_directory,
    mkdir_durable,
    read_json,
    sha256_bytes,
    write_exclusive,
)
from fs_exec.watcher import Watcher


class HardeningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.cwd = self.base / "work"
        self.cwd.mkdir()
        self.paths = TargetPaths(self.base / "relay")
        self.paths.initialize()
        self.client = Client(Target("test", self.paths.root), state_dir=self.base / "state")
        self.python = str(Path(sys.executable).resolve())
        self.policy = Policy(allowed_executables=frozenset({self.python}), cwd_roots=(self.cwd,))
        self.watcher = Watcher(self.paths.root, self.policy)

    def submit(self, code="pass", **kwargs):
        return self.client.submit(argv=[self.python, "-c", code], cwd=str(self.cwd), **kwargs)

    def rewrite(self, job, **changes):
        request = self.paths.ready(job) / "request.json"
        manifest = read_json(request)
        manifest.update(changes)
        body = canonical_json(manifest)
        request.write_bytes(body)
        commit_path = self.paths.ready(job) / "COMMIT"
        commit = read_json(commit_path)
        commit["manifest_sha256"] = sha256_bytes(body)
        commit["upload_bytes"] = sum(item["size"] for item in manifest["uploads"])
        commit_path.write_bytes(canonical_json(commit))

    def final(self, job):
        return read_final(self.paths.result(job))

    def process(self, job):
        self.assertTrue(self.watcher.process_job(job))
        return self.final(job)

    def test_canonical_policy_rejects_basename_impersonation_and_empty_policy(self):
        fake = self.cwd / Path(self.python).name
        fake.write_text("#!/bin/sh\ntouch unexpected\n")
        fake.chmod(0o700)
        for value, policy in ((str(fake), self.policy), (self.python, Policy())):
            with self.subTest(value=value), self.assertRaises(PolicyError):
                command_for({"argv": [value]}, "linux", policy)
        self.assertEqual(command_for({"argv": [Path(self.python).name, "$HOME"]}, "linux", self.policy), [self.python, "$HOME"])
        with self.assertRaises(PolicyError):
            command_for({"argv": [self.python]}, "linux", replace(self.policy, executable_sha256={self.python: "0" * 64}))

    def test_batch_and_dangerous_environment_are_denied_even_when_allowlisted(self):
        batch = self.cwd / "allowed.cmd"
        batch.touch()
        with self.assertRaises(PolicyError):
            command_for({"argv": [str(batch), "x&whoami"]}, "windows", replace(self.policy, allowed_executables=frozenset({str(batch)})))
        for name in ("LD_PRELOAD", "Path", "BASH_ENV", "PYTHONPATH", "NODE_OPTIONS", "FS_EXEC_RELAY", "API_TOKEN"):
            with self.subTest(name=name), self.assertRaises(PolicyError):
                checked_environment({name: "bad"}, replace(self.policy, environment_allowlist=frozenset({name})))
        with self.assertRaises(PolicyError):
            checked_environment({"ORDINARY": "bad"}, Policy())
        with self.assertRaises(PolicyError):
            checked_environment({"DEMO": "a", "demo": "b"}, replace(self.policy, environment_allowlist=frozenset({"DEMO", "demo"})))
        with patch.dict(os.environ, {"PYTHONPATH": "secret", "PATH": "/untrusted"}):
            env = checked_environment({}, Policy())
            self.assertNotIn("PYTHONPATH", env)
            self.assertEqual(env["PATH"], os.defpath)

    @unittest.skipUnless(os.name == "posix", "POSIX process groups")
    def test_postspawn_failure_kills_term_ignoring_descendant_before_final(self):
        ready, late = self.cwd / "ready", self.cwd / "late"
        child = f"import signal,time,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path({str(ready)!r}).touch(); time.sleep(.5); pathlib.Path({str(late)!r}).touch()"
        parent = f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{child!r}],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); time.sleep(10)"
        job = self.submit(parent)
        original = self.watcher._check_owner
        injected = False
        processes = []

        def fail(job_id):
            nonlocal injected
            if self.watcher.active and not injected:
                processes.extend(self.watcher.active.values())
                deadline = time.monotonic() + 2
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(.005)
                self.assertTrue(ready.exists())
                injected = True
                raise OSError("injected post-spawn share error")
            original(job_id)

        with patch.object(self.watcher, "_check_owner", side_effect=fail):
            self.assertEqual(self.process(job)["status"], "FAILED")
        self.assertTrue(injected)
        self.assertIsNotNone(processes[0].poll())
        self.assertFalse(late.exists())
        time.sleep(.6)
        self.assertFalse(late.exists())

    @unittest.skipUnless(os.name == "posix", "POSIX process groups")
    def test_timeout_after_leader_exit_still_kills_child_holding_pipes(self):
        late = self.cwd / "late"
        child = f"import time,pathlib; time.sleep(.6); pathlib.Path({str(late)!r}).touch()"
        parent = f"import subprocess,sys; subprocess.Popen([sys.executable,'-c',{child!r}])"
        job = self.submit(parent, command_timeout=.12)
        start = time.monotonic()
        self.assertEqual(self.process(job)["status"], "TIMED_OUT")
        self.assertLess(time.monotonic() - start, .6)
        time.sleep(.65)
        self.assertFalse(late.exists())

    def test_inline_stdin_closed_by_child_does_not_orphan_or_block(self):
        job = self.submit("import os,time; os.close(0); time.sleep(.03)", stdin="x" * 200000)
        self.assertEqual(self.process(job)["status"], "COMPLETED")

    def test_thread_start_failure_reaps_child(self):
        job = self.submit("import time; time.sleep(5)")
        real_start = threading.Thread.start
        processes = []

        def start(thread):
            if self.watcher.active:
                processes.extend(self.watcher.active.values())
                raise RuntimeError("thread startup failure")
            return real_start(thread)

        with patch.object(threading.Thread, "start", start):
            self.assertEqual(self.process(job)["status"], "FAILED")
        self.assertIsNotNone(processes[0].poll())

    def test_cleanup_failure_retains_lock_and_claim_without_false_final(self):
        job = self.submit()
        self.watcher.acquire_singleton()
        real_cleanup = self.watcher._kill_and_reap
        def cleanup(process):
            real_cleanup(process)
            raise OSError("cleanup confirmation unavailable")
        with patch.object(self.watcher, "_kill_and_reap", side_effect=cleanup):
            self.watcher.process_job(job)
        self.assertTrue(self.watcher.stop_event.is_set())
        self.assertFalse((self.paths.result(job) / "FINAL").exists())
        self.watcher.release_singleton()
        self.assertTrue((self.paths.health / "watcher.lock").exists())

    def test_invalid_owner_resource_limits_fail_at_configuration_boundary(self):
        for name, value in (("max_runtime_seconds", float("nan")), ("max_queue_age_seconds", float("inf")), ("max_upload_bytes", -1), ("max_artifact_files", -1)):
            with self.subTest(name=name), self.assertRaises(ValueError):
                replace(self.policy, **{name: value})

    @unittest.skipUnless(os.name == "posix", "POSIX executable hook")
    def test_approval_heartbeat_failure_reaps_hook_and_redacts(self):
        ready, late = self.cwd / "hook-ready", self.cwd / "hook-late"
        hook = self.cwd / "approve"
        hook.write_text(f"#!{self.python}\nimport pathlib,time,sys\npathlib.Path({str(ready)!r}).touch()\nprint('PRIVATE_INPUT',file=sys.stderr)\ntime.sleep(2)\npathlib.Path({str(late)!r}).touch()\n")
        hook.chmod(0o700)
        self.watcher = Watcher(self.paths.root, replace(self.policy, approval_hook=hook), journal=self.base / "journal")
        real_heartbeat = self.watcher.heartbeat
        processes = []
        real_popen = subprocess.Popen

        def spawn(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            processes.append(process)
            return process

        def heartbeat(state="READY"):
            if ready.exists():
                raise OSError("PRIVATE_INPUT")
            real_heartbeat(state)

        with patch("fs_exec.watcher.subprocess.Popen", side_effect=spawn), patch.object(self.watcher, "heartbeat", side_effect=heartbeat):
            result = self.process(self.submit())
        self.assertEqual(result["status"], "REJECTED")
        self.assertIsNotNone(processes[0].poll())
        self.assertNotIn("PRIVATE_INPUT", canonical_json(result).decode())
        self.assertNotIn("PRIVATE_INPUT", (self.base / "journal").read_text())
        time.sleep(1.1)
        self.assertFalse(late.exists())

    def test_cancellation_and_expiry_during_approval_never_spawn(self):
        for cancel in (True, False):
            with self.subTest(cancel=cancel):
                job = self.submit(expiry=5 if cancel else .02)
                def approve(manifest, cancel=cancel, job=job):
                    if cancel:
                        self.client.cancel(job)
                    else:
                        time.sleep(.03)
                with patch.object(self.watcher, "_approve", side_effect=approve), patch("fs_exec.watcher.subprocess.Popen") as popen:
                    self.assertEqual(self.process(job)["status"], "CANCELLED" if cancel else "EXPIRED")
                    popen.assert_not_called()

    def test_cancellation_published_with_running_marker_prevents_popen(self):
        job = self.submit()
        def write(path, body):
            write_exclusive(path, body)
            if path.name == "RUNNING":
                self.client.cancel(job)
        with patch("fs_exec.watcher.write_exclusive", side_effect=write), patch("fs_exec.watcher.subprocess.Popen") as popen:
            self.assertEqual(self.process(job)["status"], "CANCELLED")
            popen.assert_not_called()

    def test_future_queue_time_and_owner_preparation_age_cannot_bypass_policy(self):
        self.watcher.policy = replace(self.policy, max_queue_age_seconds=.02)
        job = self.submit()
        self.rewrite(job, published_ns=time.time_ns() + 10**18)
        self.assertEqual(self.process(job)["status"], "REJECTED")
        job = self.submit()
        self.rewrite(job, published_ns=time.time_ns() + 1_000_000_000)
        with patch.object(self.watcher, "_approve", side_effect=lambda m: time.sleep(.04)):
            self.assertEqual(self.process(job)["status"], "EXPIRED")

    def test_recovery_never_releases_preexisting_claim_even_without_claim_contents(self):
        job = self.submit("raise SystemExit(99)")
        (self.paths.claims / job).mkdir()
        self.watcher.recover_interrupted()
        self.assertEqual(self.final(job)["status"], "AVAILABILITY_UNKNOWN")
        self.assertFalse(self.watcher.process_job(job))
        self.assertFalse((self.paths.claims / job / "STARTED").exists())

    def test_claim_parent_fsync_precedes_start_and_spawn_and_eio_fails_closed(self):
        job = self.submit()
        events = []
        real_fsync = os.fsync
        def sync(fd):
            events.append(os.readlink(f"/proc/self/fd/{fd}"))
            real_fsync(fd)
        real_popen = subprocess.Popen
        def spawn(*args, **kwargs):
            events.append("Popen")
            return real_popen(*args, **kwargs)
        with patch("os.fsync", side_effect=sync), patch("fs_exec.watcher.subprocess.Popen", side_effect=spawn):
            self.process(job)
        self.assertLess(events.index(str(self.paths.claims)), events.index("Popen"))
        self.assertLess(events.index(str(self.paths.claims / job)), events.index("Popen"))
        for code in (errno.EIO, errno.ENOSPC, errno.EACCES):
            with self.subTest(errno=code), patch("os.fsync", side_effect=OSError(code, "injected")), self.assertRaises(OSError):
                fsync_directory(self.paths.claims)
        with patch("os.fsync", side_effect=OSError(errno.EINVAL, "unsupported")):
            fsync_directory(self.paths.claims)
        job = self.submit()
        with patch("fs_exec.watcher.fsync_directory", side_effect=OSError(errno.EIO, "injected")), patch("fs_exec.watcher.subprocess.Popen") as popen:
            self.watcher.process_job(job)
            popen.assert_not_called()
        self.assertTrue((self.paths.claims / job).exists())

    def test_durable_mkdir_syncs_all_new_namespace_ancestors(self):
        nested = self.base / "new" / "second" / "third"
        with patch("fs_exec.util.fsync_directory") as sync:
            mkdir_durable(nested)
        self.assertEqual([args.args[0] for args in sync.call_args_list], [self.base, self.base / "new", self.base / "new" / "second"])

    def test_first_final_wins_even_when_two_publishers_race(self):
        result_dir = self.paths.result("j-race")
        barrier = threading.Barrier(2)
        outcomes = []
        def write(path, body):
            if path.name == "FINAL":
                barrier.wait(2)
            write_exclusive(path, body)
        def publish(status):
            try:
                publish_final(result_dir, {"job_id": "j-race", "status": status, "fence": status})
                outcomes.append(status)
            except FileExistsError:
                outcomes.append("lost")
        with patch("fs_exec.protocol.write_exclusive", side_effect=write):
            threads = [threading.Thread(target=publish, args=(state,)) for state in ("COMPLETED", "AVAILABILITY_UNKNOWN")]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(3)
                self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes.count("lost"), 1)
        before = read_final(result_dir)
        with self.assertRaises(FileExistsError):
            publish_final(result_dir, {"job_id": "j-race", "status": "stale"})
        self.assertEqual(read_final(result_dir), before)

    def test_changed_fence_stops_launch_and_does_not_publish_as_new_owner(self):
        job = self.submit()
        def approve(manifest):
            claim_path = self.paths.claims / job / "CLAIM"
            claim = read_json(claim_path)
            claim["fence"] = "other-owner"
            claim_path.write_bytes(canonical_json(claim))
        with patch.object(self.watcher, "_approve", side_effect=approve), patch("fs_exec.watcher.subprocess.Popen") as popen:
            self.watcher.process_job(job)
            popen.assert_not_called()
        self.assertFalse((self.paths.result(job) / "FINAL").exists())

    def test_partial_final_and_delayed_generation_are_propagating(self):
        job = "j-delayed"
        result_dir = self.paths.result(job)
        result_dir.mkdir()
        (result_dir / "FINAL").write_bytes(b"")
        self.assertEqual(self.client.status(job).state, "RESULT_PROPAGATING")
        (result_dir / "FINAL").unlink()
        publish_final(result_dir, {"job_id": job, "status": "COMPLETED"})
        generation = result_dir / read_json(result_dir / "FINAL")["result_file"]
        body = generation.read_bytes()
        generation.unlink()
        self.assertEqual(self.client.status(job).state, "RESULT_PROPAGATING")
        thread = threading.Thread(target=lambda: (time.sleep(.08), generation.write_bytes(body)))
        thread.start()
        self.assertEqual(self.client.wait(job, 1).state, "COMPLETED")
        thread.join()

    def test_temporarily_unreadable_marker_ancestor_is_propagating(self):
        job = "j-unreadable"
        result_dir = self.paths.result(job)
        publish_final(result_dir, {"job_id": job, "status": "COMPLETED"})
        original = Path.exists
        def exists(path):
            if path == result_dir / "FINAL":
                raise PermissionError("transient ancestor access failure")
            return original(path)
        with patch.object(Path, "exists", exists):
            self.assertEqual(self.client.status(job).state, "RESULT_PROPAGATING")
        self.assertEqual(self.client.wait(job, .5).state, "COMPLETED")

    def stream_result(self, job, data=b"data"):
        return {"job_id": job, "status": "COMPLETED", "stdout_chunks": 1, "stderr_chunks": 0, "streams": {"stdout": [{"size": len(data), "sha256": sha256_bytes(data)}], "stderr": []}}

    def test_delayed_valid_chunk_and_artifact_ancestors_retry(self):
        job = "j-propagation"
        result_dir = self.paths.result(job)
        (result_dir / "stdout").mkdir(parents=True)
        chunk = result_dir / "stdout" / "00000000.chunk"
        chunk.write_bytes(b"")
        result = self.stream_result(job)
        result["artifacts"] = [{"name": "sub/data", "size": 4, "sha256": sha256_bytes(b"data")}]
        publish_final(result_dir, result)
        def populate():
            time.sleep(.08)
            chunk.write_bytes(b"data")
            time.sleep(.12)
            (result_dir / "artifacts" / "sub").mkdir(parents=True)
            (result_dir / "artifacts" / "sub" / "data").write_bytes(b"data")
        thread = threading.Thread(target=populate)
        thread.start()
        out = io.BytesIO()
        self.assertEqual(self.client.wait(job, 1, stream=True, stdout=out, result_timeout=.7).state, "COMPLETED")
        self.assertEqual(out.getvalue(), b"data")
        files = self.client.download_artifacts(job, self.base / "out", .7)
        self.assertEqual([file.read_bytes() for file in files], [b"data"])
        thread.join()

    def test_persistent_chunk_corruption_cannot_complete_or_emit(self):
        job = "j-corrupt"
        result_dir = self.paths.result(job)
        (result_dir / "stdout").mkdir(parents=True)
        (result_dir / "stdout" / "00000000.chunk").write_bytes(b"evil")
        publish_final(result_dir, self.stream_result(job))
        out = io.BytesIO()
        start = time.monotonic()
        status = self.client.wait(job, 1, stream=True, stdout=out, result_timeout=.08)
        self.assertEqual(status.state, "RESULT_PROPAGATING")
        self.assertIsNone(status.result)
        self.assertEqual(out.getvalue(), b"")
        self.assertGreaterEqual(time.monotonic() - start, .08)
        self.assertLess(time.monotonic() - start, .3)

    def test_persistent_artifact_corruption_never_installs_destination(self):
        job = "j-bad-artifact"
        result_dir = self.paths.result(job)
        (result_dir / "artifacts").mkdir(parents=True)
        (result_dir / "artifacts" / "data").write_bytes(b"evil")
        publish_final(result_dir, {"job_id": job, "status": "COMPLETED", "artifacts": [{"name": "data", "size": 4, "sha256": sha256_bytes(b"data")}]})
        start = time.monotonic()
        with self.assertRaises(ProtocolError):
            self.client.download_artifacts(job, self.base / "out", .08)
        self.assertGreaterEqual(time.monotonic() - start, .08)
        self.assertFalse((self.base / "out" / "data").exists())
        self.assertEqual(list((self.base / "out").iterdir()), [])

    def test_request_delayed_then_valid_is_retried_within_budget(self):
        job = self.submit()
        request = self.paths.ready(job) / "request.json"
        body = request.read_bytes()
        request.write_bytes(b"")
        thread = threading.Thread(target=lambda: (time.sleep(.08), request.write_bytes(body)))
        thread.start()
        self.assertEqual(verify_request(self.paths.ready(job), .5)["job_id"], job)
        thread.join()

    def test_partial_publication_never_exposes_exclusive_destination(self):
        path = self.base / "marker"
        real_link = os.link
        def link(src, dst, **kwargs):
            self.assertFalse(path.exists())
            real_link(src, dst, **kwargs)
            self.assertEqual(path.read_bytes(), b"complete")
        with patch("fs_exec.util.os.link", side_effect=link):
            write_exclusive(path, b"complete")

    def test_limits_reject_before_upload_hashing_or_large_json_parsing(self):
        source = self.base / "upload"
        source.write_bytes(b"12345")
        job = self.submit(uploads=[source])
        with patch("fs_exec.protocol.hash_bounded") as hashed:
            with self.assertRaises(ValueError):
                verify_request(self.paths.ready(job), 0, policy=replace(self.policy, max_upload_bytes=4))
            hashed.assert_not_called()
        with patch("fs_exec.protocol.json.loads") as parsed:
            (self.paths.ready(job) / "COMMIT").write_bytes(b" " * 4097)
            with self.assertRaises(ValueError):
                verify_request(self.paths.ready(job), 0)
            parsed.assert_not_called()

    def test_duplicate_and_negative_upload_entries_are_rejected_before_hash(self):
        for entries in ([{"name": "x", "size": -1}], [{"name": "x", "size": 0}, {"name": "X", "size": 0}]):
            job = self.submit()
            self.rewrite(job, uploads=entries)
            with patch("fs_exec.protocol.hash_bounded") as hashed, self.assertRaises(ValueError):
                verify_request(self.paths.ready(job), 0)
            hashed.assert_not_called()

    def test_bounded_copy_stops_a_file_that_grows_after_stat(self):
        source, destination = self.base / "growing", self.base / "copy"
        source.write_bytes(b"1234")
        real_fstat = os.fstat
        def stat(fd):
            info = real_fstat(fd)
            if os.readlink(f"/proc/self/fd/{fd}") == str(source):
                with source.open("ab") as handle:
                    handle.write(b"56789")
            return info
        with patch("os.fstat", side_effect=stat), self.assertRaises(ValueError):
            copy_bounded(source, destination, 6)
        self.assertFalse(destination.exists())

    def test_symlink_upload_and_download_destination_cannot_escape(self):
        source = self.base / "upload"
        source.write_bytes(b"data")
        job = self.submit(uploads=[source])
        upload = self.paths.ready(job) / "uploads" / source.name
        upload.unlink()
        upload.symlink_to(source)
        with self.assertRaises(ValueError):
            verify_request(self.paths.ready(job), 0)
        artifact = self.cwd / "data"
        artifact.write_bytes(b"data")
        job = self.submit(artifacts=["data"])
        self.process(job)
        out = self.base / "out"
        out.mkdir()
        (out / "data").symlink_to(source)
        with self.assertRaises(ValueError):
            self.client.download_artifacts(job, out, .01)
        self.assertEqual(source.read_bytes(), b"data")

    def test_unprovisioned_client_does_not_create_owner_namespace(self):
        root = self.base / "unprovisioned"
        client = Client(Target("absent", root))
        with self.assertRaises(ProtocolError):
            client.submit(argv=["anything"])
        with self.assertRaises(ProtocolError):
            client.health(probe=False)
        self.assertFalse(root.exists())

    def test_bad_job_setup_does_not_stop_next_job(self):
        bad, good = self.submit(), self.submit("print('good')")
        self.paths.result(bad).mkdir()
        (self.paths.result(bad) / "CLAIMED").write_bytes(b"hostile")
        self.watcher.process_job(bad)
        self.assertEqual(self.process(good)["status"], "COMPLETED")

    def test_slow_verification_has_fresh_heartbeat_and_busy_probe(self):
        source = self.base / "upload"
        source.write_bytes(b"bounded-file")
        job = self.submit(uploads=[source])
        entered, release = threading.Event(), threading.Event()
        from fs_exec.protocol import hash_bounded
        def slow(*args):
            entered.set()
            release.wait(5)
            return hash_bounded(*args)
        with patch("fs_exec.protocol.hash_bounded", side_effect=slow):
            worker = threading.Thread(target=self.watcher.process_job, args=(job,))
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                first = read_json(self.paths.health / "heartbeat.json")["at_ns"]
                time.sleep(1.1)
                health = self.client.health(timeout=.5)
                self.assertEqual(health["watcher_state"], "BUSY")
                self.assertTrue(health["probe"]["succeeded"])
                self.assertGreater(health["watcher"]["at_ns"], first)
                self.assertTrue(worker.is_alive())
            finally:
                release.set()
                worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.final(job)["status"], "COMPLETED")

    def test_busy_command_answers_short_health_probe(self):
        job = self.submit("import time; time.sleep(.8)")
        worker = threading.Thread(target=self.watcher.process_job, args=(job,))
        worker.start()
        try:
            self.assertTrue(self.client.wait_for_phase(job, "start", 2))
            self.assertTrue(self.client.health(timeout=.2)["probe"]["succeeded"])
        finally:
            worker.join(3)
        self.assertFalse(worker.is_alive())

    def test_health_refreshes_heartbeat_after_probe_and_cli_fails_unhealthy(self):
        old = {"at_ns": time.time_ns() - 60_000_000_000, "state": "BUSY"}
        fresh = {"at_ns": time.time_ns(), "state": "READY"}
        with patch.object(self.client, "_heartbeat", side_effect=[old, fresh]):
            self.assertEqual(self.client.health(timeout=.01)["watcher_state"], "READY")
        args = argparse.Namespace(no_probe=True, transport_timeout=".01")
        with patch("fs_exec.cli._client", return_value=self.client), patch("fs_exec.cli._json"):
            self.assertEqual(cmd_health(args), 1)


class DirectionalACLTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "linux" and shutil.which("sudo"), "Linux sudo + POSIX ACL integration")
    def test_two_uids_can_exchange_but_client_cannot_forge_results(self):
        if subprocess.run(["sudo", "-n", "-u", "nobody", "true"], capture_output=True, check=False).returncode:
            self.skipTest("passwordless sudo to nobody unavailable")
        library = ctypes.util.find_library("acl")
        if not library:
            self.skipTest("libacl unavailable")
        lib = ctypes.CDLL(library, use_errno=True)
        lib.acl_from_text.argtypes = [ctypes.c_char_p]
        lib.acl_from_text.restype = ctypes.c_void_p
        lib.acl_set_file.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_void_p]
        lib.acl_free.argtypes = [ctypes.c_void_p]
        nobody = int(subprocess.check_output(["id", "-u", "nobody"]))
        watcher_uid = os.getuid()
        def acl(path, text, default=False):
            value = lib.acl_from_text(text.encode())
            self.assertTrue(value)
            try:
                self.assertEqual(lib.acl_set_file(os.fsencode(path), 0x4000 if default else 0x8000, value), 0, ctypes.get_errno())
            finally:
                lib.acl_free(value)
        def as_client(code):
            completed = subprocess.run(["sudo", "-n", "-u", "nobody", sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            return completed.stdout.strip()
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            base.chmod(0o755)
            paths = TargetPaths(base / "relay")
            paths.initialize()
            read_acl = f"u::rwx,u:{nobody}:r-x,g::---,m::rwx,o::---"
            for path in [paths.root, *[p for p in paths.root.rglob("*") if p.is_dir()]]:
                acl(path, read_acl)
                acl(path, read_acl, True)
            for path in (paths.inbox, paths.control / "cancel", paths.health / "pings"):
                acl(path, f"u::rwx,u:{nobody}:rwx,g::---,m::rwx,o::---")
                acl(path, f"u::rwx,u:{watcher_uid}:r-x,g::---,m::rwx,o::---", True)
            python = str(Path(sys.executable).resolve())
            code = f"import os,tempfile; from pathlib import Path; from fs_exec.client import Client; from fs_exec.config import Target; os.umask(0o077); c=Client(Target('acl',Path({str(paths.root)!r}))); print(c.submit(argv=[{python!r},'-c','print(42)'],cwd={str(base)!r}))"
            job = as_client(code)
            prior_umask = os.umask(0o077)
            try:
                watcher = Watcher(paths.root, Policy(allowed_executables=frozenset({python}), cwd_roots=(base,)))
                watcher.acquire_singleton()
                self.assertTrue(watcher.process_job(job))
                check = f"from pathlib import Path; from fs_exec.client import Client; from fs_exec.config import Target; c=Client(Target('acl',Path({str(paths.root)!r}))); print(c.wait({job!r},1).result['exit_code'])"
                self.assertEqual(as_client(check), "0")
                for target in (paths.result(job) / "FINAL", paths.claims / job / "CLAIM", paths.health / "watcher.lock" / "owner.json"):
                    denied = subprocess.run(["sudo", "-n", "-u", "nobody", sys.executable, "-c", f"from pathlib import Path; Path({str(target)!r}).write_text('forged')"], capture_output=True, check=False)
                    self.assertNotEqual(denied.returncode, 0)
                watcher.release_singleton()
            finally:
                os.umask(prior_umask)
                # Client-owned immutable-by-convention jobs are not watcher-
                # writable. Only the disposable integration fixture is removed.
                subprocess.run(["sudo", "-n", "rm", "-rf", str(paths.inbox)], check=True)


if __name__ == "__main__":
    unittest.main()
