from __future__ import annotations

import io
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from fs_exec.client import Client
from fs_exec.config import Policy, Target
from fs_exec.latency import LatencyStore, percentile
from fs_exec.protocol import TargetPaths
from fs_exec.watcher import Watcher


class LatencyAndE2ETests(unittest.TestCase):
    def test_percentiles_and_auto_overhead(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LatencyStore(Path(directory) / "latency.jsonl", stale_after=60, fallback=30)
            for value in (0.1, 0.2, 0.3, 0.4, 2.0):
                store.record(value, value)
            stats = store.stats(queue_margin=2, safety_margin=1)
            self.assertEqual(percentile([1, 2, 3, 4], 0.5), 2)
            self.assertEqual(stats.p50, 0.6)
            self.assertEqual(stats.p99, 4.0)
            self.assertEqual(stats.recommended_overhead, 7.0)
            self.assertFalse(stats.stale)

    def test_stale_measurements_use_conservative_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            stats = LatencyStore(Path(directory) / "missing", fallback=30).stats()
            self.assertTrue(stats.stale)
            self.assertEqual(stats.recommended_overhead, 30)

    def test_end_to_end_watcher_health_stream_and_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, cwd = base / "relay", base / "work"
            cwd.mkdir()
            TargetPaths(root).initialize()
            policy = Policy(allowed_executables=frozenset({sys.executable}), cwd_roots=(cwd,))
            watcher = Watcher(root, policy, poll_interval=0.01)
            thread = threading.Thread(target=watcher.run)
            thread.start()
            try:
                client = Client(Target("e2e", root), state_dir=base / "state")
                job = client.submit(argv=[sys.executable, "-c", "print('e2e-ok')"], cwd=str(cwd))
                output = io.BytesIO()
                result = client.wait(job, 5, stream=True, stdout=output, stderr=io.BytesIO())
                self.assertEqual(result.result["exit_code"], 0)  # type: ignore[index]
                self.assertEqual(output.getvalue().strip(), b"e2e-ok")
                health = client.health(probe=True, timeout=2)
                self.assertEqual(health["watcher_state"], "READY")
                self.assertIsNotNone(health["probe"]["request_visibility"])
            finally:
                watcher.stop_event.set()
                thread.join(3)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
