from __future__ import annotations

import contextlib
import io
import json
import multiprocessing
import os
import shutil
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path, PurePosixPath, PureWindowsPath
from unittest.mock import patch

from fs_exec.cli import _transport
from fs_exec.client import Client
from fs_exec.config import Policy, Target
from fs_exec.protocol import (
    TargetPaths,
    publish_final,
    publish_request,
    read_final,
    verify_request,
)
from fs_exec.rally import (
    Mapping,
    Rally,
    RallyConfig,
    Rejected,
    install_file,
    main,
    object_json,
    relative,
    request_snapshot,
    result_snapshot,
    stable_read,
)
from fs_exec.util import atomic_write, canonical_json, sha256_bytes, write_exclusive
from fs_exec.watcher import Watcher


def rally_worker(config, stop):
    with Rally(config) as rally:
        while not stop.is_set():
            rally.once()
            stop.wait(0.01)


def watcher_worker(root, cwd, stop):
    watcher = Watcher(root, Policy(allowed_executables=frozenset({sys.executable}), cwd_roots=(cwd,)), poll_interval=0.01)
    watcher.stop_event = stop
    watcher.run()


class RallyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.mapping = Mapping("test", "sandbox", "host")
        self.config = RallyConfig(self.base / "drop", self.base / "shared", self.base / "state", (self.mapping,))
        self.drop = TargetPaths(self.config.sandbox_root / self.mapping.sandbox)
        self.outside = TargetPaths(self.config.outside_root / self.mapping.outside)
        self.drop.initialize()
        self.outside.initialize()
        self.client = Client(Target("test", self.drop.root), state_dir=self.base / "client-state")
        Watcher(self.outside.root, Policy()).heartbeat()

    def submit(self, job="j-test", upload=False):
        uploads = []
        if upload:
            source = self.base / "input.bin"
            source.write_bytes(b"asymmetric-upload\x00\x01")
            uploads = [(source, "input.bin")]
        return publish_request(self.drop, {"job_id": job, "argv": ["never-executed-by-rally"]}, uploads)

    def finish(self, job="j-test"):
        root = self.outside.result(job)
        data = b"output\x00\xff\n"
        artifact = b"artifact-123456789"
        write_exclusive(root / "stdout/00000000.chunk", data)
        write_exclusive(root / "artifacts/nested/report.bin", artifact)
        publish_final(root, {"protocol": 2, "job_id": job, "watcher_id": "watcher-A", "fence": "fence-A", "status": "COMPLETED", "exit_code": 7,
                             "stdout_chunks": 1, "stderr_chunks": 0, "streams": {"stdout": [{"size": len(data), "sha256": sha256_bytes(data)}], "stderr": []},
                             "artifacts": [{"name": "nested/report.bin", "size": len(artifact), "sha256": sha256_bytes(artifact)}]})
        return root

    def test_ordering_and_no_duplicate_publication(self):
        job = self.submit(upload=True)
        self.finish(job)
        seen = []
        def observe(path, data):
            seen.append(path)
            if path == self.outside.ready(job) / "COMMIT":
                self.assertEqual((path.parent / "uploads/input.bin").read_bytes(), b"asymmetric-upload\x00\x01")
                self.assertTrue((path.parent / "request.json").is_file())
            if path == self.drop.result(job) / "FINAL":
                self.assertEqual((path.parent / "stdout/00000000.chunk").read_bytes(), b"output\x00\xff\n")
                self.assertEqual((path.parent / "artifacts/nested/report.bin").read_bytes(), b"artifact-123456789")
            write_exclusive(path, data)
        with Rally(self.config) as rally, patch("fs_exec.rally.install_file", side_effect=observe):
            self.assertEqual(rally.once()["errors"], 0)
            original = list(seen)
            rally.once()
            self.assertEqual(seen, original)
            self.assertEqual(rally.status()["retained_publications"], 2)
        self.assertEqual(verify_request(self.outside.ready(job), 0)["job_id"], job)
        self.assertEqual(read_final(self.drop.result(job))["exit_code"], 7)

    def test_review_directory_durability_failure_stays_pending(self):
        from fs_exec.util import fsync_directory
        for failure_at in ("file-directory", "ancestor-directory", "marker-directory"):
            with self.subTest(failure_at=failure_at):
                job = self.submit("j-durability-" + failure_at)
                ready = self.outside.ready(job)
                gate = {"armed": failure_at != "marker-directory"}
                def fail_sync(path, failure_at=failure_at, ready=ready, gate=gate):
                    blocked = path == (self.outside.inbox if failure_at == "ancestor-directory" else ready)
                    if gate["armed"] and blocked:
                        raise OSError(5, "injected directory EIO")
                    fsync_directory(path)
                def install(path, body, ready=ready, gate=gate):
                    if path == ready / "COMMIT":
                        gate["armed"] = True
                    install_file(path, body)
                with Rally(self.config) as rally, patch("fs_exec.rally.fsync_directory", side_effect=fail_sync), patch("fs_exec.util.fsync_directory", side_effect=fail_sync), patch("fs_exec.rally.install_file", side_effect=install):
                    for _ in range(4):
                        health = rally.once()
                        self.assertGreater(health["errors"], 0)
                        self.assertEqual(health["pending"], 1)
                        self.assertFalse(rally.done(self.mapping, "outbound", f"inbox/{job}.ready/"))
                    self.assertEqual((ready / "COMMIT").exists(), failure_at == "marker-directory")
                # Restart plus a recovered filesystem must finish the same identity.
                with Rally(self.config) as rally:
                    self.assertEqual(rally.once()["pending"], 0)
                    self.assertTrue(rally.done(self.mapping, "outbound", f"inbox/{job}.ready/"))

    def test_review_transient_eio_recovery_flushes_before_marker_and_done(self):
        from fs_exec.util import fsync_directory
        job = self.submit(upload=True)
        ready = self.outside.ready(job)
        events = []
        gate = {"fail": True}
        def sync(path):
            if path == ready / "uploads" and gate["fail"]:
                gate["fail"] = False
                events.append(("failed-sync", path))
                raise OSError(5, "one transient EIO")
            fsync_directory(path)
            events.append(("synced", path))
        def install(path, body):
            events.append(("install", path))
            install_file(path, body)
        with patch("fs_exec.rally.fsync_directory", side_effect=sync), patch("fs_exec.rally.install_file", side_effect=install):
            with Rally(self.config) as rally:
                self.assertEqual(rally.once()["pending"], 1)
                self.assertFalse((ready / "COMMIT").exists())
            events.clear()
            with Rally(self.config) as rally, patch.object(rally, "log", side_effect=lambda event, **fields: events.append((event, fields.get("key")))):
                self.assertEqual(rally.once()["pending"], 0)
        marker = events.index(("install", ready / "COMMIT"))
        for file, directories in ((ready / "request.json", (ready, self.outside.inbox, self.outside.root)),
                                  (ready / "uploads/input.bin", (ready / "uploads", ready, self.outside.inbox, self.outside.root))):
            installed = events.index(("install", file))
            for directory in directories:
                self.assertIn(("synced", directory), events[installed + 1:marker])
        done = events.index(("published", f"inbox/{job}.ready/"))
        for directory in (ready, self.outside.inbox, self.outside.root):
            self.assertIn(("synced", directory), events[marker + 1:done])

    def test_review_failed_artifact_collection_still_returns_final(self):
        cwd = self.base / "work"
        cwd.mkdir()
        (cwd / "first.txt").write_text("copied before failure")
        job = self.client.submit(argv=[sys.executable, "-c", "print('executed')"], cwd=str(cwd), artifacts=["first.txt", "../invalid"])
        with Rally(self.config) as rally:
            rally.once()
        Watcher(self.outside.root, Policy(allowed_executables=frozenset({sys.executable}), cwd_roots=(cwd,))).run(once=True)
        result = read_final(self.outside.result(job))
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["artifacts"], [])
        self.assertTrue((self.outside.result(job) / "artifacts/first.txt").exists())
        with Rally(self.config) as rally:
            self.assertEqual(rally.once()["errors"], 0)
        self.assertEqual(read_final(self.drop.result(job)), result)
        self.assertFalse((self.drop.result(job) / "artifacts").exists())

    def test_review_recovered_job_ignores_partial_and_empty_artifacts(self):
        job = self.submit()
        with Rally(self.config) as rally:
            rally.once()
        watcher = Watcher(self.outside.root, Policy())
        self.assertIsNotNone(watcher._claim(job))
        root = self.outside.result(job)
        write_exclusive(root / "artifacts/nested/partial.bin", b"partial artifact")
        (root / "artifacts/empty/subdir").mkdir(parents=True)
        Watcher(self.outside.root, Policy()).run(once=True)
        recovered = read_final(root)
        self.assertEqual(recovered["status"], "AVAILABILITY_UNKNOWN")
        with Rally(self.config) as rally:
            self.assertEqual(rally.once()["errors"], 0)
        self.assertEqual(read_final(self.drop.result(job)), recovered)
        self.assertFalse((self.drop.result(job) / "artifacts").exists())
        # Unreferenced is not permission to accept dangerous tree entries.
        (root / "artifacts/alias").symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(ValueError):
            result_snapshot(root, self.config, job)

    def test_review_cancel_legal_suffixes_before_discovery(self):
        cwd = self.base / "work"
        cwd.mkdir()
        jobs = ["j-legal.tmp", "j-legal.staging", ".j-temp.123.abcdef12.tmp", "j-control"]
        for job in jobs:
            self.client.submit(job_id=job, argv=[sys.executable, "-c", "print('must not execute')"], cwd=str(cwd))
            self.client.cancel(job)
        (self.drop.inbox / ("j-unpublished." + "a" * 32 + ".staging")).mkdir()
        (self.drop.health / "pings" / ("p-" + "b" * 32 + ".staging")).mkdir()
        temporary = self.drop.control / "cancel/.j-unpublished.123.abcdef12.tmp"
        write_exclusive(temporary, canonical_json({"job_id": "j-unpublished", "at_ns": time.time_ns()}))
        with Rally(self.config) as rally:
            self.assertEqual(rally.once()["errors"], 0)
            self.assertFalse(rally.db.execute("SELECT 1 FROM known WHERE name=?", (temporary.name,)).fetchone())
        Watcher(self.outside.root, Policy(allowed_executables=frozenset({sys.executable}), cwd_roots=(cwd,))).run(once=True)
        with Rally(self.config) as rally:
            rally.once()
        for job in jobs:
            with self.subTest(job=job):
                self.assertTrue((self.outside.control / "cancel" / job).exists())
                self.assertEqual(read_final(self.drop.result(job))["status"], "CANCELLED")

    def test_partial_request_and_reordered_payload_retry_exact_path(self):
        job = self.submit(upload=True)
        payload = self.drop.ready(job) / "uploads/input.bin"
        original = payload.read_bytes()
        payload.unlink()
        with Rally(self.config) as rally:
            self.assertGreater(rally.once()["errors"], 0)
            self.assertFalse((self.outside.ready(job) / "COMMIT").exists())
            payload.write_bytes(original[:3])
            rally.once()
            self.assertFalse((self.outside.ready(job) / "COMMIT").exists())
            payload.write_bytes(original)
            # Simulate a directory listing which permanently forgets the job.
            with patch.object(rally, "discover"):
                rally.once()
            self.assertTrue((self.outside.ready(job) / "COMMIT").exists())

    def test_partial_final_and_tampered_chunks_not_exposed(self):
        job = self.submit()
        root = self.finish(job)
        chunk = root / "stdout/00000000.chunk"
        original = chunk.read_bytes()
        chunk.write_bytes(b"bad-data!")
        with Rally(self.config) as rally:
            rally.once()
            self.assertFalse((self.drop.result(job) / "FINAL").exists())
            self.assertFalse((self.drop.result(job) / "stdout/00000000.chunk").exists())
            chunk.unlink()
            rally.once()
            self.assertFalse((self.drop.result(job) / "FINAL").exists())
            chunk.write_bytes(original)
            rally.once()
            self.assertEqual(read_final(self.drop.result(job))["exit_code"], 7)

    def test_restart_replays_journal_not_mutated_source(self):
        job = self.submit(upload=True)
        marker = self.outside.ready(job) / "COMMIT"
        def interrupted(path, data):
            if path == marker:
                raise OSError("simulated crash before marker")
            write_exclusive(path, data)
        with Rally(self.config) as rally, patch("fs_exec.rally.install_file", side_effect=interrupted):
            rally.once()
            self.assertEqual(rally.status()["pending"], 1)
        (self.drop.ready(job) / "uploads/input.bin").write_bytes(b"tampered")
        with Rally(self.config) as rally:
            rally.once()
            self.assertEqual(rally.status()["pending"], 0)
        self.assertEqual(verify_request(self.outside.ready(job), 0)["uploads"][0]["size"], 19)
        # Completion identity survives destination disappearance; no resubmit.
        marker.unlink()
        with Rally(self.config) as rally:
            rally.once()
        self.assertFalse(marker.exists())

    def test_conflicting_valid_source_first_wins(self):
        job = self.submit()
        with Rally(self.config) as rally:
            rally.once()
        destination = (self.outside.ready(job) / "request.json").read_bytes()
        source = self.drop.ready(job)
        manifest = json.loads((source / "request.json").read_bytes())
        manifest["argv"] = ["changed-command"]
        body = canonical_json(manifest)
        (source / "request.json").write_bytes(body)
        commit = json.loads((source / "COMMIT").read_bytes())
        commit["manifest_sha256"] = sha256_bytes(body)
        (source / "COMMIT").write_bytes(canonical_json(commit))
        with Rally(self.config) as rally:
            self.assertGreater(rally.once()["errors"], 0)
        self.assertEqual((self.outside.ready(job) / "request.json").read_bytes(), destination)

    def test_restart_recovers_only_journal_bound_staging_links(self):
        destination = self.outside.inbox / "test-record"
        data = b"durable-complete-bytes"
        token = sha256_bytes(canonical_json([destination.name, sha256_bytes(data)]))
        temporary = destination.with_name(f".rally-{token}.tmp")
        # Crash while writing, before link: discard only this known temp name.
        temporary.write_bytes(b"partial")
        install_file(destination, data)
        self.assertEqual(stable_read(destination, 100), data)
        # Crash after link, before unlink: recover the expected two-name inode.
        os.link(destination, temporary)
        install_file(destination, data)
        self.assertFalse(temporary.exists())
        self.assertEqual(stable_read(destination, 100), data)
        # An unexpected third alias is never normalized into an accepted file.
        os.link(destination, temporary)
        other = self.base / "unexpected-link"
        os.link(destination, other)
        with self.assertRaises(Rejected):
            install_file(destination, data)
        self.assertTrue(other.exists())

    def test_destination_visibility_retry_keeps_marker_last(self):
        job = self.submit(upload=True)
        original = stable_read
        hidden = self.outside.ready(job) / "uploads/input.bin"
        def delayed(path, limit, **kwargs):
            if path == hidden:
                raise FileNotFoundError("delayed destination visibility")
            return original(path, limit, **kwargs)
        with Rally(self.config) as rally:
            with patch("fs_exec.rally.stable_read", side_effect=delayed):
                rally.once()
            self.assertFalse((self.outside.ready(job) / "COMMIT").exists())
            self.assertEqual(rally.status()["pending"], 1)
            rally.once()
            self.assertTrue((self.outside.ready(job) / "COMMIT").exists())

    def test_destination_conflict_blocks_marker(self):
        job = self.submit()
        write_exclusive(self.outside.ready(job) / "request.json", b"conflict")
        with Rally(self.config) as rally:
            rally.once()
            self.assertEqual(rally.status()["pending"], 1)
        self.assertFalse((self.outside.ready(job) / "COMMIT").exists())
        self.assertEqual((self.outside.ready(job) / "request.json").read_bytes(), b"conflict")

    def test_dry_run_is_read_only(self):
        self.submit()
        before = sorted(str(path.relative_to(self.base)) for path in self.base.rglob("*"))
        with Rally(self.config, dry_run=True) as rally:
            self.assertEqual(rally.once()["errors"], 0)
        self.assertEqual(sorted(str(path.relative_to(self.base)) for path in self.base.rglob("*")), before)

    def test_directionality_locks_claims_and_unknown_paths(self):
        job = self.submit()
        write_exclusive(self.drop.results / "evil/FINAL", b"forged")
        write_exclusive(self.drop.health / "watcher.lock/owner.json", b"fake")
        tombstone = self.outside.claims / job / "CLAIM"
        write_exclusive(tombstone, b"retained")
        with Rally(self.config) as rally:
            rally.once()
        self.assertFalse((self.outside.results / "evil/FINAL").exists())
        self.assertFalse((self.outside.health / "watcher.lock").exists())
        self.assertFalse((self.drop.claims / job).exists())
        self.assertEqual(tombstone.read_bytes(), b"retained")
        write_exclusive(self.drop.ready(job) / "unknown", b"bad")
        with self.assertRaises(Rejected):
            request_snapshot(self.drop.ready(job), self.config, job)

    def test_unknown_result_paths_fail_closed(self):
        root = self.finish()
        write_exclusive(root / "surprise", b"no")
        with self.assertRaises(Rejected):
            result_snapshot(root, self.config, "j-test")

    def test_provisional_output_waits_for_final(self):
        job = self.submit()
        identity = {"job_id": job, "watcher_id": "w", "fence": "f"}
        write_exclusive(self.outside.result(job) / "CLAIMED", canonical_json(identity))
        write_exclusive(self.outside.result(job) / "stdout/00000000.chunk", b"not-final")
        with Rally(self.config) as rally:
            rally.once()
        self.assertEqual(self.client.status(job).state, "CLAIMED")
        self.assertFalse((self.drop.result(job) / "stdout").exists())

    def test_cancel_before_request_and_client_timeout_resumable(self):
        self.client.cancel("j-later")
        with Rally(self.config) as rally:
            rally.once()
            self.assertTrue((self.outside.control / "cancel/j-later").exists())
            job = self.submit("j-later")
            self.assertIsNone(self.client.wait(job, 0.001).result)
            rally.once()
        Watcher(self.outside.root, Policy()).run(once=True)
        with Rally(self.config) as rally:
            rally.once()
        self.assertEqual(self.client.wait(job, 1).result["status"], "CANCELLED")
        self.assertEqual(len(list(self.outside.inbox.glob("*.ready"))), 1)

    def test_links_special_sparse_and_oversize(self):
        source = self.base / "regular"
        source.write_bytes(b"contents")
        link = self.base / "link"
        link.symlink_to(source)
        with self.assertRaises(ValueError):
            stable_read(link, 100)
        link.unlink()
        os.link(source, link)
        with self.assertRaises(Rejected):
            stable_read(source, 100)
        link.unlink()
        with self.assertRaises(Rejected):
            stable_read(source, 2)
        if hasattr(os, "mkfifo"):
            fifo = self.base / "fifo"
            os.mkfifo(fifo)
            with self.assertRaises(ValueError):
                stable_read(fifo, 100)
        sparse = self.base / "sparse"
        with sparse.open("wb") as handle:
            handle.seek(1024 * 1024)
            handle.write(b"x")
        if hasattr(sparse.stat(), "st_blocks") and sparse.stat().st_blocks * 512 < sparse.stat().st_size:
            with self.assertRaises(Rejected):
                stable_read(sparse, 2 * 1024 * 1024)

    def test_growing_file_race(self):
        source = self.base / "regular"
        source.write_bytes(b"contents")
        from fs_exec.util import open_regular
        @contextlib.contextmanager
        def grow(path):
            with open_regular(path) as handle:
                class Reader:
                    def fileno(self):
                        return handle.fileno()
                    def read(self, limit):
                        result = handle.read(limit)
                        with source.open("ab") as output:
                            output.write(b"growth")
                        return result
                yield Reader()
        with patch("fs_exec.rally.open_regular", grow), self.assertRaises(Rejected):
            stable_read(source, 100)

    def test_symlink_ancestor_and_destination(self):
        job = self.submit()
        self.outside.ready(job).symlink_to(self.base, target_is_directory=True)
        with Rally(self.config) as rally:
            rally.once()
        self.assertFalse((self.base / "COMMIT").exists())
        (self.drop.ready(job) / "request.json").unlink()
        (self.drop.ready(job) / "request.json").symlink_to(self.base / "missing")
        with self.assertRaises(ValueError):
            request_snapshot(self.drop.ready(job), self.config, job)

    def test_paths_portable_reject_windows_aliases_and_traversal(self):
        for value in ("..", ".", "../a", "/abs", "a//b", "a/./b", "a/../b", "a\\b", "C:/x", "C:x", "NUL", "con.txt", "a:b", "a.", "a ", "//server/share", "x/COM9.log"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                relative(value)
        for value in ("nested/report.bin", "stdout/00000000.chunk", "j-test.ready/request.json"):
            self.assertEqual(PureWindowsPath(relative(value)).parts, PurePosixPath(relative(value)).parts)

    def test_json_framing_duplicate_keys_and_incomplete(self):
        for body in (b'{"job_id":"a","job_id":"b"}', b'{"n":NaN}', b'{"n":1e999}', b'{}{}', b'{"x":', b'[]'):
            with self.subTest(body=body), self.assertRaises(ValueError):
                object_json(body)

    def test_windows_handle_sharing_and_reparse_contract(self):
        import ctypes
        from types import SimpleNamespace
        from unittest.mock import Mock

        from fs_exec.rally_windows import open_locked
        source = self.base / "source"
        source.write_bytes(b"windows-adapter")
        total = len(source.parents) + 1
        kernel = SimpleNamespace(CreateFileW=Mock(side_effect=range(1, total + 1)), GetFileInformationByHandle=Mock(), CloseHandle=Mock())
        def info(handle, pointer):
            pointer._obj.attributes = 0x10 if handle < total else 0
            return True
        kernel.GetFileInformationByHandle.side_effect = info
        with source.open("rb") as original:
            msvcrt = SimpleNamespace(open_osfhandle=lambda handle, flags: os.dup(original.fileno()))
            with patch.dict(sys.modules, {"msvcrt": msvcrt}), patch.object(ctypes, "WinDLL", return_value=kernel, create=True), patch.object(os, "O_BINARY", 0, create=True):
                with open_locked(source) as handle:
                    self.assertEqual(handle.read(), b"windows-adapter")
                calls = kernel.CreateFileW.call_args_list
                self.assertEqual([call.args[2] for call in calls], [3] * (total - 1) + [1])
                self.assertTrue(all(call.args[5] == 0x02200000 for call in calls))
                self.assertEqual([call.args[0] for call in kernel.CloseHandle.call_args_list], list(range(total - 1, 0, -1)))
                kernel.CreateFileW.reset_mock(side_effect=True)
                kernel.CreateFileW.side_effect = range(1, total + 1)
                kernel.CloseHandle.reset_mock()
                def reparse(handle, pointer):
                    pointer._obj.attributes = 0x410
                    return True
                kernel.GetFileInformationByHandle.side_effect = reparse
                with self.assertRaises(ValueError), open_locked(source):
                    pass
                kernel.CloseHandle.assert_called_once_with(1)

    def test_retention_limits_never_evict_identities(self):
        self.submit("j-first")
        config = replace(self.config, max_publications=1)
        with Rally(config) as rally:
            rally.once()
            self.submit("j-second")
            self.assertGreater(rally.once()["errors"], 0)
            self.assertTrue(rally.done(self.mapping, "outbound", "inbox/j-first.ready/"))
            self.assertFalse((self.outside.ready("j-second") / "COMMIT").exists())
            with rally.db:
                rally.db.execute("INSERT INTO samples VALUES (?,?,?)", (0, "roundtrip", 999))
            rally.once()
            self.assertEqual(rally.status()["metrics"]["roundtrip"]["samples"], 0)
            self.assertEqual(rally.status()["retained_publications"], 1)

    def test_sidecar_never_launches_a_process(self):
        self.submit()
        with patch("subprocess.Popen", side_effect=AssertionError("sidecar executed a command")), Rally(self.config) as rally:
            self.assertEqual(rally.once()["errors"], 0)

    def test_corrupt_journal_payload_is_not_replayed(self):
        job = self.submit()
        with Rally(self.config) as rally:
            with patch("fs_exec.rally.install_file", side_effect=OSError("interrupted")):
                rally.once()
            with rally.db:
                rally.db.execute("UPDATE files SET body=? WHERE ordinal=0", (b"corrupt persisted bytes",))
        with Rally(self.config) as rally:
            self.assertGreater(rally.once()["errors"], 0)
            self.assertEqual(rally.status()["pending"], 1)
        self.assertFalse(self.outside.ready(job).exists())

    def test_scalar_records_obey_publication_byte_limit(self):
        self.client.cancel("j-cancel")
        with Rally(replace(self.config, max_publication_bytes=10)) as rally:
            self.assertGreater(rally.once()["errors"], 0)
        self.assertFalse((self.outside.control / "cancel/j-cancel").exists())

    def test_limits_and_mapping_rebind(self):
        self.submit(upload=True)
        for config in (replace(self.config, max_files=2), replace(self.config, max_file_bytes=10), replace(self.config, max_publication_bytes=10), replace(self.config, max_pending_bytes=10)):
            with Rally(config) as rally:
                self.assertGreater(rally.once()["errors"], 0)
        with self.assertRaises(Rejected), Rally(replace(self.config, targets=(Mapping("renamed", "sandbox", "host"),))):
            pass
        for options in ({"outside_root": self.config.sandbox_root}, {"poll_min": float("nan")}, {"max_files": -1}, {"backend": "rsync"}, {"targets": (Mapping("x", "..", "x"),)}, {"targets": (Mapping("x", "sandbox", "host", "both"),)}):
            with self.assertRaises(ValueError):
                replace(self.config, **options)

    def test_singleton_and_restart_unlock(self):
        with Rally(self.config), self.assertRaises(Rejected), Rally(self.config):
            pass
        with Rally(self.config) as rally:
            self.assertEqual(rally.once()["pending"], 0)

    @unittest.skipUnless(sys.platform == "linux" and shutil.which("sudo"), "three-UID POSIX ACL integration")
    def test_three_uid_acl_directionality(self):
        import ctypes
        import ctypes.util
        import subprocess
        for account in ("nobody", "daemon"):
            if subprocess.run(["sudo", "-n", "-u", account, "true"], capture_output=True, check=False).returncode:
                self.skipTest(f"sudo to {account} unavailable")
        library = ctypes.util.find_library("acl")
        if not library:
            self.skipTest("libacl unavailable")
        lib = ctypes.CDLL(library, use_errno=True)
        lib.acl_from_text.argtypes = [ctypes.c_char_p]
        lib.acl_from_text.restype = ctypes.c_void_p
        lib.acl_set_file.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_void_p]
        lib.acl_free.argtypes = [ctypes.c_void_p]
        client_uid = int(subprocess.check_output(["id", "-u", "nobody"]))
        relay_uid = int(subprocess.check_output(["id", "-u", "daemon"]))
        watcher_uid = os.getuid()
        def acl(path, grants):
            value = lib.acl_from_text(f"u::rwx,{grants},g::---,m::rwx,o::---".encode())
            self.assertTrue(value)
            try:
                for mode in (0x8000, 0x4000):
                    self.assertEqual(lib.acl_set_file(os.fsencode(path), mode, value), 0, ctypes.get_errno())
            finally:
                lib.acl_free(value)
        self.base.chmod(0o755)
        self.config.sandbox_root.chmod(0o755)
        self.config.outside_root.chmod(0o755)
        for root in (self.drop.root, self.outside.root):
            for directory in [root, *(path for path in root.rglob("*") if path.is_dir())]:
                acl(directory, f"u:{client_uid}:r-x,u:{relay_uid}:r-x")
        for directory in (self.drop.inbox, self.drop.control / "cancel", self.drop.health / "pings"):
            acl(directory, f"u:{client_uid}:rwx,u:{relay_uid}:r-x")
        for directory in (self.drop.results, self.drop.health, self.drop.health / "pongs"):
            acl(directory, f"u:{client_uid}:r-x,u:{relay_uid}:rwx")
        for directory in (self.outside.inbox, self.outside.control / "cancel", self.outside.health / "pings"):
            acl(directory, f"u:{relay_uid}:rwx,u:{watcher_uid}:r-x")
        # Recreate the heartbeat under the ACL, rather than chmod an old file.
        (self.outside.health / "heartbeat.json").unlink()
        Watcher(self.outside.root, Policy()).heartbeat()
        self.config.state_dir.mkdir()
        subprocess.run(["sudo", "-n", "chown", str(relay_uid), str(self.config.state_dir)], check=True)
        self.addCleanup(lambda: subprocess.run(["sudo", "-n", "rm", "-rf", str(self.base)], check=True))
        def run(account, code):
            return subprocess.run(["sudo", "-n", "-u", account, sys.executable, "-c", code], capture_output=True, text=True, cwd=Path(__file__).resolve().parents[1], check=False)
        job = "j-acl"
        submitted = run("nobody", f"from fs_exec.client import Client; from fs_exec.config import Target; from pathlib import Path; Client(Target('t',Path({str(self.drop.root)!r}))).submit(argv=['nothing'],job_id='{job}')")
        self.assertEqual(submitted.returncode, 0, submitted.stderr)
        relay_code = f"from fs_exec.rally import *; from pathlib import Path; c=RallyConfig(Path({str(self.config.sandbox_root)!r}),Path({str(self.config.outside_root)!r}),Path({str(self.config.state_dir)!r}),(Mapping('test','sandbox','host'),)); r=Rally(c); r.__enter__(); h=r.once(); r.__exit__(); assert h['errors']==0,h"
        forwarded = run("daemon", relay_code)
        self.assertEqual(forwarded.returncode, 0, forwarded.stderr)
        Watcher(self.outside.root, Policy()).run(once=True)
        returned = run("daemon", relay_code)
        self.assertEqual(returned.returncode, 0, returned.stderr)
        checked = run("nobody", f"from fs_exec.protocol import read_final; from pathlib import Path; assert read_final(Path({str(self.drop.result(job))!r}))['status']=='REJECTED'")
        self.assertEqual(checked.returncode, 0, checked.stderr)
        for account, path in (("nobody", self.drop.result(job) / "FINAL"), ("nobody", self.drop.health / "heartbeat.json"), ("daemon", self.outside.result(job) / "FINAL"), ("daemon", self.outside.claims / job / "CLAIM")):
            denied = run(account, f"from pathlib import Path; Path({str(path)!r}).write_text('forged')")
            self.assertNotEqual(denied.returncode, 0)

    def test_heartbeat_does_not_regress_and_invalid_json_retries(self):
        with Rally(self.config) as rally:
            rally.once()
        destination = self.drop.health / "heartbeat.json"
        original = destination.read_bytes()
        source = self.outside.health / "heartbeat.json"
        value = json.loads(original)
        value["at_ns"] -= 1_000_000
        atomic_write(source, canonical_json(value))
        with Rally(self.config) as rally:
            rally.once()
        self.assertEqual(destination.read_bytes(), original)
        source.write_bytes(b"{")
        with Rally(self.config) as rally:
            self.assertGreater(rally.once()["errors"], 0)
            source.write_bytes(original)
            self.assertEqual(rally.once()["errors"], 0)

    def test_latency_fallback_and_no_double_counting(self):
        path = self.drop.health / "rally.json"
        atomic_write(path, canonical_json({"at_ns": time.time_ns(), "recommended_overhead": 80}))
        self.assertEqual(_transport(self.client, "auto"), 80)
        for rtt in (0.1, 0.3, 0.7):
            self.client.latency.record(100, 0, round_trip=rtt)
        self.assertAlmostEqual(_transport(self.client, "auto"), 4.7)
        self.assertEqual(self.client.latency.stats().p99, 0.7)
        self.assertEqual(_transport(self.client, "9"), 9)
        configured = Client(replace(self.client.target, transport_timeout=12), state_dir=self.base / "other-state")
        self.assertEqual(_transport(configured, "auto"), 12)
        for bad in (b'{', b'[]', canonical_json({"at_ns": 0, "recommended_overhead": 800}), canonical_json({"at_ns": time.time_ns(), "recommended_overhead": -1})):
            atomic_write(path, bad)
            self.assertEqual(configured.transport_overhead(), 12)

    def test_cli_modes_and_exit_codes(self):
        config = self.base / "rally.toml"
        config.write_text(f'''[rally]
sandbox_root = {json.dumps(str(self.config.sandbox_root))}
outside_root = {json.dumps(str(self.config.outside_root))}
state_dir = {json.dumps(str(self.config.state_dir))}
[[targets]]
name = "test"
sandbox = "sandbox"
outside = "host"
direction = "protocol-v2"
''')
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["--config", str(config), "dry-run"]), 0)
            self.assertFalse(self.config.state_dir.exists())
            self.assertEqual(main(["--config", str(config), "once"]), 0)
            self.assertEqual(main(["--config", str(config), "status"]), 0)
            self.assertEqual(main(["--config", str(config), "health"]), 0)
            value = json.loads((self.config.state_dir / "health.json").read_bytes())
            value["at_ns"] = 0
            atomic_write(self.config.state_dir / "health.json", canonical_json(value))
            self.assertEqual(main(["--config", str(config), "health"]), 1)
            self.assertEqual(main(["--config", str(self.base / "missing"), "once"]), 2)

    def test_multiprocess_e2e_artifacts_cancel_health_and_resume(self):
        cwd = self.base / "work"
        cwd.mkdir()
        context = multiprocessing.get_context("spawn")
        stop = context.Event()
        processes = [context.Process(target=rally_worker, args=(self.config, stop)), context.Process(target=watcher_worker, args=(self.outside.root, cwd, stop))]
        for process in processes:
            process.start()
        try:
            job = self.client.submit(argv=[sys.executable, "-c", "from pathlib import Path; Path('artifact.txt').write_text('multi-process artifact'); print('rally-e2e'); raise SystemExit(7)"], cwd=str(cwd), artifacts=["artifact.txt"])
            output = io.BytesIO()
            result = self.client.wait(job, 15, stream=True, stdout=output, stderr=io.BytesIO())
            self.assertEqual(result.result["exit_code"], 7)
            self.assertEqual(output.getvalue(), b"rally-e2e\n")
            self.client.download_artifacts(job, self.base / "download")
            self.assertEqual((self.base / "download/artifact.txt").read_text(), "multi-process artifact")
            for _ in range(3):
                health = self.client.health(timeout=5)
                self.assertTrue(health["probe"]["succeeded"])
                self.assertGreater(health["probe"]["round_trip"], 0)
            self.assertEqual(health["latency"]["samples"], 3)
            self.assertIsNotNone(health["relay"])
            job2 = self.client.submit(argv=[sys.executable, "-c", "import time; time.sleep(10)"], cwd=str(cwd))
            self.assertIsNone(self.client.wait(job2, 0.001).result)
            self.assertTrue(self.client.wait_for_phase(job2, "start", 5))
            self.client.cancel(job2)
            resumed = self.client.wait(job2, 10)
            self.assertEqual(resumed.result["status"], "CANCELLED")
            self.assertEqual(len(list(self.outside.inbox.glob("*.ready"))), 2)
            print(f"MULTIPROCESS E2E: exit=7 stdout=rally-e2e artifact=verified probes=3 cancellation=CANCELLED requests=2 rtt_p99={health['latency']['p99']:.4f}s")
        finally:
            stop.set()
            for process in processes:
                process.join(5)
                if process.is_alive():
                    process.terminate()
                    process.join(2)
                self.assertEqual(process.exitcode, 0)


if __name__ == "__main__":
    unittest.main()
