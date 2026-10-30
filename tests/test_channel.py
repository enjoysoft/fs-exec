from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from fs_exec.channel import (
    Broker,
    ChannelConfig,
    Client,
    Participant,
    provision,
    read_events,
)
from fs_exec.rally import Rejected, install_file
from fs_exec.util import canonical_json, sha256_bytes


def chat(text, mentions=None, reply_to=None):
    return {"kind": "chat", "text": text, "mentions": mentions or [], "reply_to": reply_to}


def channel_broker_worker(config, stop):
    with Broker(config) as broker:
        while not stop.is_set():
            result = broker.once()
            if result["errors"]:
                raise AssertionError(result)
            stop.wait(0.005)


def channel_client_worker(config, author, state, count):
    for index in range(count):
        # Reopen for every message, including an ambiguous caller retry.
        message = chat(f"{author}:{index}", ["coordinator"])
        with Client(config, author, state) as client:
            seq = client.queue(message, f"message-{index}")
        with Client(config, author, state) as client:
            if client.queue(message, f"message-{index}") != seq:
                raise AssertionError("retry changed durable identity")
            client.sync()


def channel_crash_worker(config, phase):
    from fs_exec.util import atomic_write

    def deny_execution(event, args):
        if event in {"subprocess.Popen", "os.system", "os.exec", "os.spawn", "os.fork", "os.posix_spawn"}:
            raise AssertionError("channel attempted process execution")

    sys.addaudithook(deny_execution)
    if phase == "client":
        def interrupted(path, body):
            install_file(path, body)
            os._exit(81)
        with Client(config, "coordinator", config.state_dir.parent / "coordinator-private") as client:
            client.queue({"kind": "assign", "task": "crash-task", "assignee": "agent-a", "text": "work"}, "crash")
            with patch("fs_exec.channel.install_file", side_effect=interrupted):
                client.sync()
    else:
        with Broker(config) as broker:
            if phase == "transaction":
                # Kill after task mutation but before its event insertion/commit.
                broker.db.set_trace_callback(lambda sql: os._exit(81) if sql.startswith("INSERT INTO events") else None)
            def interrupted_file(path, body):
                if phase == "journal":
                    os._exit(81)
                install_file(path, body)
            def interrupted_head(path, body):
                atomic_write(path, body)
                if phase == "head":
                    os._exit(81)
            with patch("fs_exec.channel.install_file", side_effect=interrupted_file), patch("fs_exec.channel.atomic_write", side_effect=interrupted_head):
                broker.once()


class ChannelTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.config = self.make_config("default")
        self.key = 0

    def make_config(self, directory, **limits):
        base = self.base / directory
        participants = tuple(
            Participant(author, base / author, "coordinator" if author == "coordinator" else "agent")
            for author in ("coordinator", "agent-a", "agent-b", "agent-c", "agent-d")
        )
        config = ChannelConfig("group", base / "broker", participants, **limits)
        provision(config)
        return config

    def state(self, author, config=None):
        return (config or self.config).state_dir.parent / (author + "-private")

    def inbox(self, author, seq=1, config=None):
        return (config or self.config).participant(author).root / "inbox" / f"{seq:016d}.json"

    def feed(self, author, seq=1, config=None):
        return (config or self.config).participant(author).root / "feed" / f"{seq:016d}.json"

    def send(self, author, message, config=None):
        config = config or self.config
        self.key += 1
        with Client(config, author, self.state(author, config)) as client:
            seq = client.queue(message, f"key-{self.key}")
            client.sync()
        return seq

    def transact(self, author, message, accepted=True):
        author_seq = self.send(author, message)
        with Broker(self.config) as broker:
            result = broker.once()
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["processed"], 1)
        event = read_events(self.config, author)[-1]["event"]
        self.assertEqual(event["author_seq"], author_seq)
        self.assertEqual(event["accepted"], accepted)
        if accepted:
            self.assertEqual(event["message"], message)
            self.assertIsNone(event["reason"])
        else:
            self.assertIsNone(event["message"])
            self.assertTrue(event["reason"])
        return event["seq"], result

    def assert_fanout(self, config, count):
        reference = read_events(config, "coordinator")
        self.assertEqual(len(reference), count)
        previous = "0" * 64
        for seq, envelope in enumerate(reference, 1):
            self.assertEqual(set(envelope), {"event", "sha256"})
            event = envelope["event"]
            self.assertEqual(event["seq"], seq)
            self.assertEqual(event["channel"], config.channel)
            self.assertEqual(event["protocol"], 1)
            self.assertEqual(event["previous"], previous)
            self.assertEqual(envelope["sha256"], sha256_bytes(canonical_json(event)))
            previous = envelope["sha256"]
        for participant in config.participants:
            self.assertEqual(read_events(config, participant.name), reference)
            for seq in range(1, count + 1):
                self.assertEqual(self.feed(participant.name, seq, config).read_bytes(),
                                 self.feed("coordinator", seq, config).read_bytes())
        return reference

    def test_five_participant_ordering_fanout_without_directory_listings(self):
        for participant in reversed(self.config.participants):
            for index in range(3):
                self.send(participant.name, chat(f"{participant.name}:{index}"))
            (participant.root / "inbox/garbage.json").write_bytes(b"not a request")
        # Listings may be empty/stale on a shared filesystem; exact paths win.
        with patch("os.listdir", return_value=[]), patch("os.scandir", return_value=iter(())), Broker(self.config) as broker:
            for _ in range(3):
                result = broker.once()
                self.assertEqual(result["processed"], 5)
                self.assertEqual(result["errors"], [])
            self.assertEqual(broker.once()["processed"], 0)
            records = self.assert_fanout(self.config, 15)
        for participant in self.config.participants:
            events = [r["event"] for r in records if r["event"]["author"] == participant.name]
            self.assertEqual([e["author_seq"] for e in events], [1, 2, 3])
            self.assertEqual([e["message"]["text"] for e in events],
                             [f"{participant.name}:{index}" for index in range(3)])
            for event in events:
                self.assertEqual(event["request_sha256"], sha256_bytes(self.inbox(participant.name, event["author_seq"]).read_bytes()))

    def test_durable_queue_idempotency_and_conflicts_across_restarts(self):
        message = chat("same bytes after caller restart")
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            with self.assertRaises(TypeError):
                client.queue(message)
            self.assertEqual(client.queue(message, "retry"), 1)
            self.assertEqual(client.pending(), [1])
        self.assertFalse(self.inbox("agent-a").exists())
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            self.assertEqual(client.queue(dict(reversed(list(message.items()))), "retry"), 1)
            with self.assertRaises(Rejected):
                client.queue(chat("different bytes"), "retry")
            self.assertEqual(client.pending(), [1])
            self.assertEqual(client.sync(), [1])
            self.assertEqual(client.pending(), [])
        for _ in range(2):
            with Client(self.config, "agent-a", self.state("agent-a")) as client:
                self.assertEqual(client.queue(message, "retry"), 1)
                self.assertEqual(client.sync(), [])
            with Broker(self.config) as broker:
                broker.once()
                self.assertEqual(broker.status()["events"], 1)
        self.assert_fanout(self.config, 1)
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            with self.assertRaises(Rejected):
                client.queue(chat("different bytes"), "retry")
            self.assertEqual(client.queue(message, "different-key"), 2)

    def test_private_state_singletons_and_identity_binding(self):
        with Broker(self.config) as broker:
            with self.assertRaises(OSError), Broker(self.config):
                pass
            self.assertEqual(broker.once()["events"], 0)
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            client.queue(chat("locked"), "lock")
            with self.assertRaises(OSError), Client(self.config, "agent-a", self.state("agent-a")):
                pass
            self.assertEqual(client.pending(), [1])
        with self.assertRaises(Rejected), Client(self.config, "agent-b", self.state("agent-a")):
            pass
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            self.assertEqual(client.queue(chat("locked"), "lock"), 1)
            client.sync()
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["events"], 1)

    def test_client_publication_failure_after_visible_file_recovers(self):
        def interrupted(path, body):
            install_file(path, body)
            raise OSError("lost publication acknowledgement")

        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            client.queue(chat("recover me"), "retry")
            with patch("fs_exec.channel.install_file", side_effect=interrupted), self.assertRaises(OSError):
                client.sync()
            self.assertEqual(client.pending(), [1])
        original = self.inbox("agent-a").read_bytes()
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["events"], 1)
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            self.assertEqual(client.queue(chat("recover me"), "retry"), 1)
            self.assertEqual(client.sync(), [1])
            self.assertEqual(client.pending(), [])
        self.assertEqual(self.inbox("agent-a").read_bytes(), original)
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["processed"], 0)
        self.assert_fanout(self.config, 1)

    def test_client_conflicting_destination_keeps_pending_until_operator_repair(self):
        self.inbox("agent-a").write_bytes(b"conflicting bytes")
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            client.queue(chat("original"), "retry")
            with self.assertRaises(Rejected):
                client.sync()
            self.assertEqual(client.pending(), [1])
        self.assertEqual(self.inbox("agent-a").read_bytes(), b"conflicting bytes")
        self.inbox("agent-a").unlink()
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            self.assertEqual(client.sync(), [1])
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["events"], 1)

    def test_broker_partial_fanout_replays_journal_and_blocks_new_intake(self):
        self.send("coordinator", chat("journal original"))
        self.send("agent-d", chat("must wait"))
        blocked = self.feed("agent-b")

        def interrupted(path, body):
            if path == blocked:
                raise OSError("feed temporarily unavailable")
            install_file(path, body)

        with Broker(self.config) as broker, patch("fs_exec.channel.install_file", side_effect=interrupted):
            for _ in range(2):
                with self.assertRaises(OSError):
                    broker.once()
                self.assertEqual(broker.status()["events"], 1)
                self.assertEqual(broker.status()["pending"], 1)
        self.assertTrue(self.feed("coordinator").exists())
        for participant in self.config.participants:
            self.assertFalse((participant.root / "feed/HEAD").exists())
        # Accepted journal bytes, not a later source mutation, drive recovery.
        self.inbox("coordinator").write_bytes(b"mutated after acceptance")
        with Broker(self.config) as broker:
            result = broker.once()
            self.assertEqual(result["pending"], 0)
            self.assertEqual(result["events"], 2)
            self.assertEqual(result["processed"], 1)
        records = self.assert_fanout(self.config, 2)
        self.assertEqual(records[0]["event"]["message"]["text"], "journal original")

    def test_broker_conflicting_feed_never_overwritten_and_recovery_is_deduplicated(self):
        self.send("agent-a", chat("original"))
        conflict = self.feed("agent-c")
        conflict.write_bytes(b"do not overwrite")
        with Broker(self.config) as broker:
            with self.assertRaises(Rejected):
                broker.once()
            self.assertEqual(broker.status()["pending"], 1)
        self.assertEqual(conflict.read_bytes(), b"do not overwrite")
        conflict.unlink()
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["pending"], 0)
            self.assertEqual(broker.once()["events"], 1)
        self.assert_fanout(self.config, 1)

    def test_abrupt_death_preserves_atomic_tasks_ids_and_fanout(self):
        context = multiprocessing.get_context("spawn")
        message = {"kind": "assign", "task": "crash-task", "assignee": "agent-a", "text": "work"}
        for phase in ("client", "transaction", "journal", "head"):
            with self.subTest(phase=phase):
                config = self.make_config("death-" + phase)
                if phase != "client":
                    self.send("coordinator", message, config)
                process = context.Process(target=channel_crash_worker, args=(config, phase))
                process.start()
                process.join(15)
                if process.is_alive():
                    process.terminate()
                    process.join(5)
                    self.fail("crash worker hung")
                self.assertEqual(process.exitcode, 81)
                if phase == "client":
                    with Client(config, "coordinator", self.state("coordinator", config)) as client:
                        self.assertEqual(client.queue(message, "crash"), 1)
                        self.assertEqual(client.pending(), [1])
                        client.sync()
                with Broker(config) as recovered:
                    before = recovered.status()
                    if phase == "transaction":
                        self.assertEqual(before["events"], 0)
                        self.assertEqual(before["tasks"], [])
                    recovered.once()
                    result = recovered.once()
                    self.assertEqual(result["events"], 1)
                    self.assertEqual(result["pending"], 0)
                    self.assertEqual(result["tasks"], [{"task": "crash-task", "assignee": "agent-a", "status": "assigned", "revision": 1}])
                events = self.assert_fanout(config, 1)
                self.assertTrue(events[0]["event"]["accepted"])

    def test_persistent_directory_eio_keeps_existing_event_pending_across_restart(self):
        from fs_exec.util import fsync_directory

        self.send("coordinator", chat("persist before HEAD"))
        blocked = self.config.participant("agent-b").root

        def fail_barrier(path):
            if path == blocked:
                raise OSError(5, "persistent directory EIO")
            fsync_directory(path)

        with patch("fs_exec.channel.fsync_directory", side_effect=fail_barrier):
            for _ in range(3):
                with Broker(self.config) as broker:
                    with self.assertRaises(OSError):
                        broker.once()
                    self.assertEqual(broker.status()["pending"], 1)
                    self.assertEqual(broker.status()["events"], 1)
                self.assertTrue(self.feed("agent-b").exists())
                self.assertFalse((blocked / "feed/HEAD").exists())
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["pending"], 0)
        self.assert_fanout(self.config, 1)

    @unittest.skipUnless(sys.platform == "linux" and shutil.which("sudo"), "six-UID Linux ACL integration")
    def test_six_uid_directional_acl_and_private_journal_isolation(self):
        import ctypes
        import ctypes.util

        accounts = ("nobody", "bin", "sys", "games", "mail")
        broker_account = "daemon"
        for account in (*accounts, broker_account):
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
        broker_uid = int(subprocess.check_output(["id", "-u", broker_account]))

        def acl(path, grants):
            value = lib.acl_from_text(f"u::rwx,{grants},g::---,m::rwx,o::---".encode())
            self.assertTrue(value)
            try:
                for mode in (0x8000, 0x4000):
                    self.assertEqual(lib.acl_set_file(os.fsencode(path), mode, value), 0, ctypes.get_errno())
            finally:
                lib.acl_free(value)

        def private(path, account):
            path.mkdir(mode=0o700)
            subprocess.run(["sudo", "-n", "chown", account, str(path)], check=True)

        self.base.chmod(0o755)
        self.config.state_dir.parent.chmod(0o755)
        private(self.config.state_dir, broker_account)
        self.addCleanup(lambda: subprocess.run(["sudo", "-n", "rm", "-rf", str(self.base)], check=True))
        for participant, account in zip(self.config.participants, accounts):
            uid = int(subprocess.check_output(["id", "-u", account]))
            acl(participant.root, f"u:{uid}:r-x,u:{broker_uid}:r-x")
            acl(participant.root / "inbox", f"u:{uid}:rwx,u:{broker_uid}:r-x")
            acl(participant.root / "feed", f"u:{uid}:r-x,u:{broker_uid}:rwx")
            (participant.root / "CHANNEL").chmod(0o644)
            private(self.state(participant.name), account)
        config_path = self.write_cli_config()
        prefix = f"import os; os.umask(0o077); from fs_exec.channel import *; from pathlib import Path; c=ChannelConfig.load(Path({str(config_path)!r})); "

        def run(account, code):
            return subprocess.run(["sudo", "-n", "-u", account, sys.executable, "-c", prefix + code],
                                  capture_output=True, text=True, timeout=20, check=False)

        for participant, account in zip(self.config.participants, accounts):
            code = f"client=Client(c,{participant.name!r},Path({str(self.state(participant.name))!r})); client.queue({chat(participant.name)!r},'hello'); client.sync(); client.store.close()"
            result = run(account, code)
            self.assertEqual(result.returncode, 0, result.stderr)
        result = run(broker_account, "b=Broker(c); r=b.once(); assert r['events']==5 and not r['errors'],r; b.store.close()")
        self.assertEqual(result.returncode, 0, result.stderr)
        for index, (participant, account) in enumerate(zip(self.config.participants, accounts)):
            result = run(account, f"assert len(read_events(c,{participant.name!r}))==5")
            self.assertEqual(result.returncode, 0, result.stderr)
            other = self.config.participants[(index + 1) % 5]
            forbidden = [
                f"Path({str(participant.root / 'feed/HEAD')!r}).write_text('forged')",
                f"Path({str(self.feed(participant.name))!r}).unlink()",
                f"Path({str(participant.root / 'feed')!r}).rename(Path({str(participant.root / 'renamed')!r}))",
                f"Path({str(other.root / 'inbox/0000000000000002.json')!r}).write_text('forged')",
                f"Path({str(self.inbox(other.name))!r}).read_bytes()",
                f"Path({str(self.config.state_dir / 'channel.db')!r}).read_bytes()",
                f"Path({str(self.state(other.name) / 'channel.db')!r}).read_bytes()",
            ]
            for code in forbidden:
                result = run(account, code)
                self.assertNotEqual(result.returncode, 0, code)
                self.assertIn("PermissionError", result.stderr)
            result = run(broker_account, f"Path({str(self.inbox(participant.name))!r}).write_text('forged')")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("PermissionError", result.stderr)

    def test_mentions_replies_and_monotonic_acknowledgements(self):
        first, _ = self.transact("coordinator", chat("attention", ["agent-a", "agent-d"]))
        reply, _ = self.transact("agent-a", chat("reply", ["coordinator"], first))
        _, status = self.transact("agent-d", {"kind": "ack", "upto": reply})
        self.assertEqual(status["acks"], {"agent-d": reply})
        self.transact("agent-d", {"kind": "ack", "upto": reply})
        for invalid in (chat("bad mention", ["unknown"]), chat("duplicate", ["agent-a", "agent-a"]),
                        chat("future reply", reply_to=999), chat("boolean reply", reply_to=True),
                        {"kind": "ack", "upto": first}, {"kind": "ack", "upto": 999}):
            with self.subTest(message=invalid):
                _, status = self.transact("agent-d", invalid, accepted=False)
                self.assertEqual(status["acks"], {"agent-d": reply})
        # Rejected messages consume exactly one author/global slot, not a stream stall.
        self.transact("agent-d", chat("still usable"))
        self.assert_fanout(self.config, 11)

    def test_task_authorization_revision_transitions_and_terminal_identity(self):
        assignment = {"kind": "assign", "task": "build", "assignee": "agent-a", "text": "build it"}
        self.transact("agent-b", assignment, accepted=False)
        revision, result = self.transact("coordinator", assignment)
        self.assertEqual(result["tasks"], [{"task": "build", "assignee": "agent-a", "status": "assigned", "revision": revision}])

        def update(author, status, expected, accepted=True):
            return self.transact(author, {"kind": "status", "task": "build", "revision": expected,
                                          "status": status, "text": "progress"}, accepted)

        for author, status in (("agent-b", "running"), ("coordinator", "running"), ("agent-a", "completed")):
            _, result = update(author, status, revision, False)
            self.assertEqual(result["tasks"][0]["revision"], revision)
            self.assertEqual(result["tasks"][0]["status"], "assigned")
        running, _ = update("agent-a", "running", revision)
        _, result = update("agent-a", "blocked", revision, False)
        self.assertEqual(result["tasks"][0]["revision"], running)
        blocked, _ = update("agent-a", "blocked", running)
        running, _ = update("agent-a", "running", blocked)
        completed, _ = update("agent-a", "completed", running)
        for author in ("agent-a", "coordinator"):
            _, result = update(author, "cancelled", completed, False)
            self.assertEqual(result["tasks"][0]["status"], "completed")
        self.transact("coordinator", assignment, accepted=False)
        second = {**assignment, "task": "cancel-me"}
        revision, _ = self.transact("coordinator", second)
        _, result = self.transact("coordinator", {"kind": "status", "task": "cancel-me", "revision": revision,
                                                 "status": "cancelled", "text": "stop"})
        self.assertEqual(next(t for t in result["tasks"] if t["task"] == "cancel-me")["status"], "cancelled")

    def test_assignee_failed_and_cancelled_paths_remain_terminal(self):
        paths = (("blocked", "failed"), ("running", "failed"), ("cancelled",),
                 ("running", "cancelled"), ("blocked", "cancelled"))
        for index, statuses in enumerate(paths):
            task = f"task-{index}"
            with self.subTest(statuses=statuses):
                revision, _ = self.transact("coordinator", {"kind": "assign", "task": task,
                                                           "assignee": "agent-a", "text": "work"})
                for status in statuses:
                    revision, result = self.transact("agent-a", {"kind": "status", "task": task,
                                                                "revision": revision, "status": status, "text": "progress"})
                    self.assertEqual(next(t for t in result["tasks"] if t["task"] == task),
                                     {"task": task, "assignee": "agent-a", "status": status, "revision": revision})
                self.transact("agent-a", {"kind": "status", "task": task, "revision": revision,
                                          "status": "running", "text": "cannot reopen"}, False)

    def test_presence_expiry_is_informational_and_zero_ttl_withdraws(self):
        now = 1_000_000_000_000
        with patch("fs_exec.channel.time.time_ns", return_value=now):
            _, result = self.transact("agent-a", {"kind": "presence", "session": "session-a", "ttl": 2})
        self.assertEqual(result["presence"], [{"author": "agent-a", "session": "session-a",
                                             "expires_ns": now + 2_000_000_000, "online": True}])
        with patch("fs_exec.channel.time.time_ns", return_value=now + 2_000_000_000), Broker(self.config) as broker:
            self.assertFalse(broker.status()["presence"][0]["online"])
            self.assertEqual(broker.status()["events"], 1)
        # Offline agents remain authorized; leases do not fence their actions.
        self.transact("agent-a", chat("offline does not revoke membership"))
        revision, _ = self.transact("coordinator", {"kind": "assign", "task": "offline-task", "assignee": "agent-a", "text": "work"})
        self.transact("agent-a", {"kind": "status", "task": "offline-task", "revision": revision, "status": "running", "text": "working"})
        _, result = self.transact("agent-a", {"kind": "presence", "session": "session-b", "ttl": 0})
        self.assertFalse(result["presence"][0]["online"])
        self.assertEqual(result["presence"][0]["session"], "session-b")
        for ttl in (-1, True, self.config.max_lease_seconds + 1):
            _, result = self.transact("agent-a", {"kind": "presence", "session": "session-c", "ttl": ttl}, False)
            self.assertEqual(result["presence"][0]["session"], "session-b")

    def test_invalid_message_schema_is_rejected_without_poisoning_stream(self):
        messages = ([], {"kind": "unknown"}, {"kind": "chat", "text": "missing fields"},
                    {**chat("extra"), "execute": "never"}, chat("   "),
                    {**chat("wrong mentions"), "mentions": "agent-a"},
                    {"kind": [], "text": "wrong kind"},
                    {"kind": "assign", "task": "../escape", "assignee": "agent-a", "text": "bad task"})
        for message in messages:
            with self.subTest(message=message):
                self.transact("coordinator", message, False)
        self.transact("coordinator", chat("valid after rejection"))

    def test_nonfinite_json_never_enters_durable_queue(self):
        with Client(self.config, "agent-a", self.state("agent-a")) as client:
            for value in (float("nan"), float("inf"), -float("inf")):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    client.queue({**chat("bad number"), "reply_to": value}, "retry")
                self.assertEqual(client.pending(), [])
            self.assertEqual(client.queue(chat("valid retry"), "retry"), 1)

    def test_pending_limit_preserves_retry_and_failed_queue_does_not_consume_id(self):
        config = self.make_config("bounded-pending", max_pending=2)
        with Client(config, "agent-a", self.state("agent-a", config)) as client:
            self.assertEqual(client.queue(chat("one"), "one"), 1)
            self.assertEqual(client.queue(chat("two"), "two"), 2)
            self.assertEqual(client.queue(chat("one"), "one"), 1)
            with self.assertRaises(Rejected):
                client.queue(chat("three"), "three")
            self.assertEqual(client.pending(), [1, 2])
            self.assertEqual(client.sync(), [1, 2])
            self.assertEqual(client.queue(chat("three"), "three"), 3)

    def test_message_byte_limit_is_exact_and_counts_utf8(self):
        config = self.make_config("bounded-bytes", max_message_bytes=512)
        message = chat("")
        overhead = len(canonical_json({"protocol": 1, "channel": config.channel, "author": "agent-a", "seq": 1, "message": message}))
        exact = chat("x" * (512 - overhead))
        with Client(config, "agent-a", self.state("agent-a", config)) as client:
            for too_large in (chat(exact["text"] + "x"), chat("é" * len(exact["text"]))):
                with self.assertRaises(Rejected):
                    client.queue(too_large, "size")
                self.assertEqual(client.pending(), [])
            self.assertEqual(client.queue(exact, "size"), 1)
            client.sync()
        self.assertEqual(self.inbox("agent-a", config=config).stat().st_size, 512)
        with Broker(config) as broker:
            self.assertEqual(broker.once()["errors"], [])
        self.assert_fanout(config, 1)

    def test_event_cap_never_evicts_or_reuses_durable_identities(self):
        config = self.make_config("bounded-events", max_events=2)
        for author in ("coordinator", "agent-a", "agent-b"):
            self.send(author, chat(author), config)
        for _ in range(2):
            with Broker(config) as broker:
                result = broker.once()
                self.assertEqual(result["events"], 2)
                self.assertEqual([error["participant"] for error in result["errors"]], ["agent-b"])
        records = self.assert_fanout(config, 2)
        self.assertEqual([r["event"]["author"] for r in records], ["coordinator", "agent-a"])
        self.assertTrue(self.inbox("agent-b", config=config).exists())
        with Client(config, "agent-d", self.state("agent-d", config)) as client:
            client.queue(chat("first"), "first")
            client.queue(chat("second"), "second")
            client.sync()
            with self.assertRaises(Rejected):
                client.queue(chat("third"), "third")
            self.assertEqual(client.queue(chat("first"), "first"), 1)

    def test_malformed_json_and_identity_errors_preserve_exact_slot_for_retry(self):
        self.send("agent-a", chat("valid"))
        path = self.inbox("agent-a")
        original = path.read_bytes()
        request = json.loads(original)
        malformed = [b'{', b'[]', b'{}{}', b'{"x":NaN}', b'{"x":1e999}', b'{"x":1,"x":2}']
        malformed.append(canonical_json({**request, "message": chat("x" * self.config.max_message_bytes)}))
        for changes in ({"author": "agent-b"}, {"channel": "other"}, {"protocol": True}, {"seq": True}, {"seq": 2}, {"unexpected": 1}):
            malformed.append(canonical_json({**request, **changes}))
        with Broker(self.config) as broker:
            for body in malformed:
                with self.subTest(body=body[:100]):
                    path.write_bytes(body)
                    result = broker.once()
                    self.assertEqual(result["processed"], 0)
                    self.assertEqual(result["events"], 0)
                    self.assertEqual([error["participant"] for error in result["errors"]], ["agent-a"])
                    self.assertEqual(path.read_bytes(), body)
            path.write_bytes(original)
            self.assertEqual(broker.once()["processed"], 1)
        self.assert_fanout(self.config, 1)

    def test_gap_waits_for_exact_path_and_does_not_block_other_authors(self):
        self.send("agent-a", chat("first"))
        self.send("agent-a", chat("second"))
        first = self.inbox("agent-a").read_bytes()
        self.inbox("agent-a").unlink()
        self.send("agent-b", chat("independent"))
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["events"], 1)
            self.assertEqual(broker.once()["processed"], 0)
        self.inbox("agent-a").write_bytes(first)
        with patch("os.listdir", return_value=[]), Broker(self.config) as broker:
            self.assertEqual(broker.once()["processed"], 1)
            self.assertEqual(broker.once()["processed"], 1)
        records = self.assert_fanout(self.config, 3)
        self.assertEqual([(r["event"]["author"], r["event"]["author_seq"]) for r in records],
                         [("agent-b", 1), ("agent-a", 1), ("agent-a", 2)])

    def test_unsafe_request_links_are_rejected_without_following_target(self):
        self.send("agent-a", chat("safe"))
        request = self.inbox("agent-a")
        original = request.read_bytes()
        target = self.base / "outside.json"
        target.write_bytes(original)
        request.unlink()
        for link in ("symlink", "hardlink"):
            with self.subTest(link=link):
                if link == "symlink":
                    request.symlink_to(target)
                else:
                    os.link(target, request)
                with Broker(self.config) as broker:
                    result = broker.once()
                    self.assertEqual(result["events"], 0)
                    self.assertEqual(len(result["errors"]), 1)
                self.assertEqual(target.read_bytes(), original)
                request.unlink()
        request.write_bytes(original)
        with Broker(self.config) as broker:
            self.assertEqual(broker.once()["events"], 1)

    def test_identifiers_and_private_state_paths_cannot_escape_or_overlap(self):
        for bad in ("../escape", "/absolute", "a/b", "a\\b", "CON", "con", "nul", "a:b", "a.", "", "A", "a" * 49):
            with self.subTest(identifier=bad):
                with self.assertRaises(ValueError):
                    replace(self.config, channel=bad)
                with Client(self.config, "agent-a", self.state("agent-a")) as client:
                    with self.assertRaises(ValueError):
                        client.queue(chat("no escape"), bad)
                    self.assertEqual(client.pending(), [])
        for state in (self.config.state_dir, self.config.state_dir / "nested", self.config.participant("agent-a").root,
                      self.config.participant("agent-b").root / "private", self.config.state_dir.parent, Path("relative")):
            with self.subTest(state=state), self.assertRaises(ValueError), Client(self.config, "agent-a", state):
                pass
        alias = self.base / "alias"
        alias.symlink_to(self.config.participant("agent-a").root, target_is_directory=True)
        with self.assertRaises(ValueError), Client(self.config, "agent-a", alias / "private"):
            pass

    def test_feed_pagination_missing_files_and_cursor_regression_fail_closed(self):
        for index in range(3):
            self.transact("agent-a", chat(str(index)))
        self.assertEqual([r["event"]["seq"] for r in read_events(self.config, "agent-b", limit=2)], [1, 2])
        self.assertEqual([r["event"]["seq"] for r in read_events(self.config, "agent-b", after=2, limit=1)], [3])
        self.assertEqual(read_events(self.config, "agent-b", after=3), [])
        head = self.config.participant("agent-b").root / "feed/HEAD"
        original_head = head.read_bytes()
        head.unlink()
        with self.assertRaises(ValueError):
            read_events(self.config, "agent-b", after=3)
        head.write_bytes(original_head)
        for seq, after in ((2, 0), (2, 2)):
            path = self.feed("agent-b", seq)
            original = path.read_bytes()
            path.unlink()
            with self.assertRaises((OSError, ValueError)):
                read_events(self.config, "agent-b", after=after)
            path.write_bytes(original)
        old = json.loads(self.feed("agent-b", 1).read_bytes())
        head.write_bytes(canonical_json({"channel": self.config.channel, "seq": 1, "sha256": old["sha256"]}))
        with self.assertRaises(ValueError):
            read_events(self.config, "agent-b", after=3)
        head.write_bytes(original_head)
        self.assertEqual(read_events(self.config, "agent-b", after=3), [])
        for after, limit in ((-1, 1), (True, 1), (0, 0), (0, self.config.page_size + 1)):
            with self.subTest(after=after, limit=limit), self.assertRaises(ValueError):
                read_events(self.config, "agent-b", after, limit)

    def test_corrupt_event_hash_chain_identity_and_head_are_rejected(self):
        self.transact("agent-a", chat("first"))
        self.transact("agent-a", chat("second"))
        path = self.feed("agent-b", 2)
        original = path.read_bytes()
        for field, value in (("previous", "f" * 64), ("seq", 1), ("channel", "other"), ("protocol", 2), ("message", chat("tampered"))):
            with self.subTest(field=field):
                record = json.loads(original)
                record["event"][field] = value
                # Rehash structural corruption to exercise more than checksum validation.
                if field != "message":
                    record["sha256"] = sha256_bytes(canonical_json(record["event"]))
                path.write_bytes(canonical_json(record))
                with self.assertRaises(ValueError):
                    read_events(self.config, "agent-b")
        path.write_bytes(original)
        head = path.parent / "HEAD"
        original_head = head.read_bytes()
        value = json.loads(original_head)
        value["sha256"] = "0" * 64
        head.write_bytes(canonical_json(value))
        with self.assertRaises(ValueError):
            read_events(self.config, "agent-b")
        head.write_bytes(original_head)
        self.assert_fanout(self.config, 2)

    def write_cli_config(self):
        path = self.base / "channel.toml"
        lines = ["[channel]", 'channel = "group"', f"state_dir = {json.dumps(str(self.config.state_dir))}"]
        for participant in self.config.participants:
            lines.extend(["[[participants]]", f"name = {json.dumps(participant.name)}",
                          f"root = {json.dumps(str(participant.root))}", f"role = {json.dumps(participant.role)}"])
        path.write_text("\n".join(lines) + "\n")
        return path

    def cli(self, config_path, *args, stdin=None, expected=0):
        result = subprocess.run([sys.executable, "-m", "fs_exec.channel", "--config", str(config_path), *args],
                                input=stdin, capture_output=True, text=True, timeout=15, check=False)
        self.assertEqual(result.returncode, expected, result.stderr)
        return [json.loads(line) for line in result.stdout.splitlines()], result

    def test_subprocess_cli_adapter_retries_receives_and_never_executes_text(self):
        config_path = self.write_cli_config()
        rows, _ = self.cli(config_path, "init")
        self.assertTrue(rows[0]["acl_setup_required"])
        marker = self.base / "must-not-exist"
        request = json.dumps({"id": "retry", "message": chat(f"touch {marker}", ["agent-b"])})
        args = ("adapter", "--participant", "agent-a", "--state", str(self.state("agent-a")))
        rows, _ = self.cli(config_path, *args, stdin=request)
        self.assertEqual(rows, [{"queued": [1]}, {"events": []}])
        rows, _ = self.cli(config_path, "once")
        self.assertEqual(rows[0]["events"], 1)
        rows, _ = self.cli(config_path, *args, stdin=request)
        self.assertEqual(rows[0], {"queued": [1]})
        self.assertEqual(len(rows[1]["events"]), 1)
        rows, _ = self.cli(config_path, *args, "--after", "1", stdin="{}")
        self.assertEqual(rows, [{"queued": []}, {"events": []}])
        self.cli(config_path, *args, stdin=json.dumps({"id": "retry", "message": chat("conflict")}), expected=2)
        for body in ('{"message": {}}', '{"id":"x","message":{},"command":"execute"}', '{', "x" * (self.config.max_message_bytes + 1)):
            with self.subTest(body=body[:80]):
                self.cli(config_path, *args, stdin=body, expected=2)
        rows, _ = self.cli(config_path, "pending", "--participant", "agent-a", "--state", str(self.state("agent-a")))
        self.assertEqual(rows, [{"pending": []}])
        rows, _ = self.cli(config_path, "receive", "--participant", "agent-d")
        self.assertEqual(rows, read_events(self.config, "agent-d"))
        rows, _ = self.cli(config_path, "once")
        self.assertEqual(rows[0]["events"], 1)
        self.assertEqual(rows[0]["processed"], 0)
        self.assertFalse(marker.exists())

    def test_subprocess_send_reports_durable_id_before_failure_and_adapter_recovers(self):
        config_path = self.write_cli_config()
        self.inbox("agent-a").write_bytes(b"publication conflict")
        identity = ("--participant", "agent-a", "--state", str(self.state("agent-a")))
        message = chat("durable despite failed send")
        rows, result = self.cli(config_path, "send", *identity, "--id", "retry",
                                "--message", json.dumps(message), expected=2)
        self.assertEqual(rows, [{"channel": self.config.channel, "author": "agent-a",
                                 "author_seq": 1, "state": "queued"}])
        self.assertTrue(json.loads(result.stderr)["error"])
        rows, _ = self.cli(config_path, "pending", *identity)
        self.assertEqual(rows, [{"pending": [1]}])
        self.inbox("agent-a").unlink()
        rows, _ = self.cli(config_path, "adapter", *identity, stdin="{}")
        self.assertEqual(rows, [{"queued": []}, {"events": []}])
        rows, _ = self.cli(config_path, "pending", *identity)
        self.assertEqual(rows, [{"pending": []}])
        self.cli(config_path, "once")
        rows, _ = self.cli(config_path, "send", *identity, "--id", "retry", "--message", json.dumps(message))
        self.assertEqual(rows[0]["author_seq"], 1)
        rows, _ = self.cli(config_path, "once")
        self.assertEqual(rows[0]["processed"], 0)
        self.assert_fanout(self.config, 1)

    def test_multiprocess_five_participants_restart_retry_and_fanout(self):
        context = multiprocessing.get_context("spawn")
        stop = context.Event()
        broker = context.Process(target=channel_broker_worker, args=(self.config, stop))
        clients = [context.Process(target=channel_client_worker,
                                   args=(self.config, participant.name, self.state(participant.name), 3))
                   for participant in self.config.participants]
        started = []
        try:
            broker.start()
            started.append(broker)
            for client in clients:
                client.start()
                started.append(client)
            for client in clients:
                client.join(20)
                self.assertFalse(client.is_alive(), "client did not finish")
                self.assertEqual(client.exitcode, 0)
            stop.set()
            broker.join(10)
            self.assertFalse(broker.is_alive(), "broker did not stop")
            self.assertEqual(broker.exitcode, 0)
        finally:
            stop.set()
            for process in started:
                process.join(2)
                if process.is_alive():
                    process.terminate()
                    process.join(5)
        # A broker restart drains any requests published just before shutdown.
        with Broker(self.config) as restarted:
            for _ in range(3):
                self.assertEqual(restarted.once()["errors"], [])
            self.assertEqual(restarted.once()["processed"], 0)
            self.assertEqual(restarted.status()["events"], 15)
        records = self.assert_fanout(self.config, 15)
        for participant in self.config.participants:
            events = [r["event"] for r in records if r["event"]["author"] == participant.name]
            self.assertEqual([e["author_seq"] for e in events], [1, 2, 3])
            self.assertEqual([e["message"]["text"] for e in events], [f"{participant.name}:{index}" for index in range(3)])
            self.assertTrue(all(e["accepted"] for e in events))
            with Client(self.config, participant.name, self.state(participant.name)) as client:
                self.assertEqual(client.pending(), [])


if __name__ == "__main__":
    unittest.main()
