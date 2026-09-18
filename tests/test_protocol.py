from __future__ import annotations

import io
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from fs_exec.client import Client
from fs_exec.config import Target
from fs_exec.protocol import ProtocolError, TargetPaths, publish_final, publish_request, read_final, verify_request
from fs_exec.util import exact_wait, now_ns


class ProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.paths = TargetPaths(self.root)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def manifest(self, job_id: str = "j-test") -> dict[str, object]:
        return {"job_id": job_id, "mode": "argv", "argv": ["echo", "ok"], "expires_ns": now_ns() + 10_000_000_000}

    def test_publication_commits_before_atomic_ready_rename(self) -> None:
        real_replace = os.replace
        observed: list[tuple[str, bool]] = []

        def replace(source: object, destination: object) -> None:
            observed.append((str(destination), Path(source).joinpath("COMMIT").is_file()))
            real_replace(source, destination)

        with mock.patch("fs_exec.protocol.os.replace", side_effect=replace):
            publish_request(self.paths, self.manifest())
        self.assertEqual(observed, [(str(self.paths.ready("j-test")), True)])
        self.assertFalse(list(self.paths.inbox.glob("*.staging")))

    def test_request_and_upload_checksums_are_verified(self) -> None:
        upload = self.root / "data.txt"
        upload.write_text("original")
        publish_request(self.paths, self.manifest(), [(upload, "data.txt")])
        verify_request(self.paths.ready("j-test"))
        (self.paths.ready("j-test") / "uploads" / "data.txt").write_text("tampered")
        with self.assertRaises(ProtocolError):
            verify_request(self.paths.ready("j-test"))

    def test_final_result_checksum_is_verified(self) -> None:
        result_dir = self.paths.result("j-test")
        publish_final(result_dir, {"job_id": "j-test", "status": "COMPLETED"})
        self.assertEqual(read_final(result_dir)["status"], "COMPLETED")
        (result_dir / "result.json").write_text("{}")
        with self.assertRaises(ProtocolError):
            read_final(result_dir)

    def test_exact_marker_poll_tolerates_delayed_visibility(self) -> None:
        marker = self.root / "exact-marker"
        thread = threading.Thread(target=lambda: (time.sleep(0.08), marker.write_text("ok")))
        thread.start()
        self.assertTrue(exact_wait(marker, 1, initial=0.01))
        thread.join()

    def test_status_reports_all_visibility_phases(self) -> None:
        client = Client(Target("test", self.root), state_dir=self.root / "state")
        self.assertEqual(client.status("j-x").state, "AWAITING_VISIBILITY")
        publish_request(self.paths, self.manifest("j-x"))
        self.assertIn(client.status("j-x").state, {"VISIBLE_UNCLAIMED", "AVAILABILITY_UNKNOWN"})
        result = self.paths.result("j-x")
        result.mkdir(parents=True)
        (result / "CLAIMED").write_text("{}")
        self.assertEqual(client.status("j-x").state, "AVAILABILITY_UNKNOWN")
        (result / "result.json").write_text("{}")
        self.assertEqual(client.status("j-x").state, "RESULT_PROPAGATING")

    def test_wait_polls_numbered_chunk_even_when_final_arrives_first(self) -> None:
        client = Client(Target("test", self.root), state_dir=self.root / "state")
        result_dir = self.paths.result("j-delayed")
        publish_final(result_dir, {"job_id": "j-delayed", "status": "COMPLETED", "exit_code": 0, "stdout_chunks": 1, "stderr_chunks": 0})

        def delayed_chunk() -> None:
            time.sleep(0.08)
            (result_dir / "stdout").mkdir()
            (result_dir / "stdout" / "00000000.chunk").write_bytes(b"delayed")

        thread = threading.Thread(target=delayed_chunk)
        thread.start()
        output = io.BytesIO()
        status = client.wait("j-delayed", 1, stream=True, stdout=output, stderr=io.BytesIO(), result_timeout=0.5)
        thread.join()
        self.assertEqual(status.state, "COMPLETED")
        self.assertEqual(output.getvalue(), b"delayed")


if __name__ == "__main__":
    unittest.main()
