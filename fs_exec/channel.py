"""A non-executing, single-sequencer group channel with directional mailboxes.

The sequencer's private journal and filesystem ACLs authenticate authors. Hashes
only frame integrity. Neither presence nor task status fences an external agent.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

from .rally import Rejected, install_file, object_json, stable_read
from .util import (
    atomic_write,
    canonical_json,
    check_path,
    fsync_directory,
    mkdir_durable,
    sha256_bytes,
)


def name(value: str) -> str:
    # Lowercase names avoid case aliases on SMB and Windows.
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,47}", value):
        raise Rejected("expected lowercase identifier (1..48 characters)")
    if value in {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}:
        raise Rejected("reserved identifier")
    return value


def integer(value: int, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise Rejected(f"expected integer in {low}..{high}")
    return value


@dataclass(frozen=True)
class Participant:
    name: str
    root: Path
    role: str = "agent"


@dataclass(frozen=True)
class ChannelConfig:
    channel: str
    state_dir: Path
    participants: tuple[Participant, ...]
    max_events: int = 10000
    max_message_bytes: int = 16384
    max_pending: int = 32
    max_lease_seconds: int = 300
    page_size: int = 100

    def __post_init__(self):
        name(self.channel)
        integer(len(self.participants), 2, 64)
        if len({name(p.name) for p in self.participants}) != len(self.participants):
            raise ValueError("duplicate participant")
        if sum(p.role == "coordinator" for p in self.participants) != 1 or any(p.role not in {"coordinator", "agent"} for p in self.participants):
            raise ValueError("exactly one coordinator and known roles required")
        roots = [self.state_dir, *(p.root for p in self.participants)]
        for root in roots:
            check_path(root)
            if not root.is_absolute() or Path(os.path.abspath(root)) != root:
                raise ValueError("absolute normalized paths required")
        for i, left in enumerate(roots):
            for right in roots[i + 1:]:
                if left == right or left in right.parents or right in left.parents or (left.exists() and right.exists() and os.path.samefile(left, right)):
                    raise ValueError("mailboxes and private state must not overlap")
        integer(self.max_events, 1, 1000000)
        integer(self.max_message_bytes, 512, 1048576)
        integer(self.max_pending, 1, 1024)
        integer(self.max_lease_seconds, 1, 3600)
        integer(self.page_size, 1, 1000)

    @classmethod
    def load(cls, path: Path):
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
        return cls(**{**raw["channel"], "state_dir": Path(raw["channel"]["state_dir"]),
                      "participants": tuple(Participant(**{**p, "root": Path(p["root"])}) for p in raw["participants"])})

    def participant(self, author: str) -> Participant:
        return next((p for p in self.participants if p.name == author), None) or self._unknown()

    @staticmethod
    def _unknown():
        raise Rejected("unknown participant")

    def manifest(self) -> bytes:
        # Paths are local mount names, not protocol identities.
        return canonical_json({"protocol": 1, "channel": self.channel,
                               "participants": [{"name": p.name, "role": p.role} for p in self.participants],
                               **{k: v for k, v in asdict(self).items() if k.startswith("max_") or k == "page_size"}})


def provision(config: ChannelConfig) -> None:
    """Administrator only; assign directional ACLs AFTER provisioning."""
    for participant in config.participants:
        for directory in (participant.root, participant.root / "inbox", participant.root / "feed"):
            mkdir_durable(directory)
        publish(participant.root / "CHANNEL", config.manifest(), participant.root)


def publish(path: Path, body: bytes, root: Path) -> None:
    try:
        install_file(path, body)
    except FileExistsError:
        if stable_read(path, len(body)) != body:
            raise Rejected("immutable destination conflict")
    if stable_read(path, len(body)) != body:
        raise Rejected("publication readback mismatch")
    # Existing equal bytes can follow a failed fsync. Always retry barriers.
    for directory in (path.parent, *path.parent.parents):
        fsync_directory(directory)
        if directory == root:
            break


class Store:
    """Private LOCAL SQLite state, locked for the entire process lifetime."""

    def __init__(self, path: Path, binding: bytes):
        check_path(path)
        fresh = not path.exists()
        mkdir_durable(path)
        if fresh and os.name != "nt":
            path.chmod(0o700)
        for leaf in ("lock", "channel.db", "channel.db-journal"):
            check_path(path / leaf)
            if (path / leaf).exists():
                self._check_file(path / leaf)
        self.lock = (path / "lock").open("a+b")
        self.db = None
        try:
            if os.name == "nt":
                import msvcrt
                if self.lock.tell() == 0:
                    self.lock.write(b"0")
                    self.lock.flush()
                self.lock.seek(0)
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.db = sqlite3.connect(path / "channel.db")
            self.db.execute("PRAGMA journal_mode=DELETE")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, author TEXT, author_seq INTEGER,
                    body BLOB NOT NULL, hash TEXT NOT NULL, published INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(author,author_seq));
                CREATE TABLE IF NOT EXISTS streams (author TEXT PRIMARY KEY, seq INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, assignee TEXT, status TEXT, revision INTEGER);
                CREATE TABLE IF NOT EXISTS acks (author TEXT PRIMARY KEY, upto INTEGER);
                CREATE TABLE IF NOT EXISTS presence (author TEXT PRIMARY KEY, session TEXT, expires_ns INTEGER);
                CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY, body BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS sent (id TEXT PRIMARY KEY, seq INTEGER UNIQUE, hash TEXT NOT NULL);
            """)
            old = self.db.execute("SELECT value FROM meta WHERE key='binding'").fetchone()
            if old and old[0] != binding:
                raise Rejected("state belongs to a different namespace/configuration")
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO meta VALUES ('binding',?)", (binding,))
            fsync_directory(path)
            fsync_directory(path.parent)
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _check_file(path):
        import stat
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise Rejected("unsafe private state file")

    def close(self):
        if self.db is not None:
            self.db.close()
        self.lock.close()


def require_mailbox(config: ChannelConfig, participant: Participant):
    for directory in (participant.root, participant.root / "inbox", participant.root / "feed"):
        check_path(directory)
        if not directory.is_dir():
            raise Rejected("administrator must provision mailboxes and ACLs")
    if stable_read(participant.root / "CHANNEL", 65536) != config.manifest():
        raise Rejected("channel manifest mismatch")


def event_file(root: Path, seq: int) -> Path:
    return root / "feed" / f"{seq:016d}.json"


def decode_event(body: bytes, config: ChannelConfig, seq: int) -> dict:
    record = object_json(body)
    if set(record) != {"event", "sha256"} or not isinstance(record["event"], dict):
        raise Rejected("invalid event framing")
    event = record["event"]
    if sha256_bytes(canonical_json(event)) != record["sha256"] or event.get("seq") != seq or event.get("channel") != config.channel or event.get("protocol") != 1:
        raise Rejected("event integrity/identity mismatch")
    return record


def read_events(config: ChannelConfig, author: str, after: int = 0, limit: int | None = None) -> list[dict]:
    participant = config.participant(author)
    require_mailbox(config, participant)
    integer(after, 0, config.max_events)
    limit = config.page_size if limit is None else integer(limit, 1, config.page_size)
    try:
        head = object_json(stable_read(participant.root / "feed/HEAD", 4096))
    except FileNotFoundError:
        if after:
            raise Rejected("feed head unavailable; retain cursor")
        return []
    if set(head) != {"channel", "seq", "sha256"} or head["channel"] != config.channel:
        raise Rejected("invalid feed head")
    last = integer(head["seq"], 1, config.max_events)
    if after > last:
        raise Rejected("head regressed; retain cursor")
    previous = "0" * 64
    if after:
        previous = decode_event(stable_read(event_file(participant.root, after), config.max_message_bytes + 4096), config, after)["sha256"]
    result = []
    for seq in range(after + 1, min(last, after + limit) + 1):
        record = decode_event(stable_read(event_file(participant.root, seq), config.max_message_bytes + 4096), config, seq)
        if record["event"].get("previous") != previous:
            raise Rejected("noncontiguous or corrupt feed; retain cursor")
        previous = record["sha256"]
        result.append(record)
    if min(last, after + limit) == last and previous != head["sha256"]:
        raise Rejected("head integrity mismatch")
    return result


class Broker:
    def __init__(self, config: ChannelConfig):
        self.config = config
        for participant in config.participants:
            require_mailbox(config, participant)
        binding = canonical_json({"manifest": object_json(config.manifest()), "roots": [str(p.root) for p in config.participants], "role": "broker"})
        self.store = Store(config.state_dir, binding)
        self.db = self.store.db

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.store.close()

    def _message(self, author: str, message: dict, seq: int, at: int) -> None:
        """Validate and apply semantic effects within the event transaction."""
        if not isinstance(message, dict):
            raise Rejected("message must be an object")
        kind = message.get("kind")
        fields = {"chat": {"text", "mentions", "reply_to"}, "ack": {"upto"},
                  "presence": {"session", "ttl"}, "assign": {"task", "assignee", "text"},
                  "status": {"task", "revision", "status", "text"}}
        if not isinstance(kind, str) or kind not in fields or set(message) - {"kind"} != fields[kind]:
            raise Rejected("unknown kind or missing/extra fields")
        if "text" in message and (not isinstance(message["text"], str) or not message["text"].strip()):
            raise Rejected("nonempty text required")
        if kind == "chat":
            mentions = message["mentions"]
            if not isinstance(mentions, list) or len(mentions) > len(self.config.participants) or any(not isinstance(m, str) for m in mentions) or len(set(mentions)) != len(mentions):
                raise Rejected("invalid mentions")
            for mention in mentions:
                self.config.participant(mention)
            if message["reply_to"] is not None:
                integer(message["reply_to"], 1, seq - 1)
        elif kind == "ack":
            upto = integer(message["upto"], 0, seq - 1)
            old = self.db.execute("SELECT upto FROM acks WHERE author=?", (author,)).fetchone()
            if old and upto < old[0]:
                raise Rejected("acknowledgement cannot regress")
            self.db.execute("INSERT OR REPLACE INTO acks VALUES (?,?)", (author, upto))
        elif kind == "presence":
            session = name(message["session"])
            ttl = integer(message["ttl"], 0, self.config.max_lease_seconds)
            self.db.execute("INSERT OR REPLACE INTO presence VALUES (?,?,?)", (author, session, at + ttl * 1000000000))
        elif kind == "assign":
            if self.config.participant(author).role != "coordinator":
                raise Rejected("only coordinator assigns tasks")
            task = name(message["task"])
            self.config.participant(message["assignee"])
            if self.db.execute("SELECT 1 FROM tasks WHERE id=?", (task,)).fetchone():
                raise Rejected("task identity already assigned; never reuse")
            self.db.execute("INSERT INTO tasks VALUES (?,?,?,?)", (task, message["assignee"], "assigned", seq))
        elif kind == "status":
            task = name(message["task"])
            revision = integer(message["revision"], 1, seq - 1)
            row = self.db.execute("SELECT assignee,status,revision FROM tasks WHERE id=?", (task,)).fetchone()
            status = message["status"]
            transitions = {"assigned": {"running", "blocked", "cancelled"}, "running": {"blocked", "completed", "failed", "cancelled"}, "blocked": {"running", "failed", "cancelled"}}
            coordinator = self.config.participant(author).role == "coordinator"
            if not row or revision != row[2] or not isinstance(status, str) or status not in transitions.get(row[1], set()):
                raise Rejected("stale revision or invalid task transition")
            if author != row[0] and not (coordinator and status == "cancelled"):
                raise Rejected("only assignee reports status; coordinator may cancel")
            self.db.execute("UPDATE tasks SET status=?,revision=? WHERE id=?", (status, seq, task))

    def _accept(self, participant: Participant) -> bool:
        row = self.db.execute("SELECT seq FROM streams WHERE author=?", (participant.name,)).fetchone()
        author_seq = (row[0] if row else 0) + 1
        path = participant.root / "inbox" / f"{author_seq:016d}.json"
        try:
            body = stable_read(path, self.config.max_message_bytes)
        except FileNotFoundError:
            return False
        request = object_json(body)
        if stable_read(path, self.config.max_message_bytes) != body:
            raise Rejected("request changed during validation")
        if set(request) != {"protocol", "channel", "author", "seq", "message"} or type(request["protocol"]) is not int or request["protocol"] != 1 or request["channel"] != self.config.channel or request["author"] != participant.name or type(request["seq"]) is not int or request["seq"] != author_seq:
            raise Rejected("request identity mismatch; preserve slot for operator inspection")
        last = self.db.execute("SELECT seq,hash FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        seq = (last[0] if last else 0) + 1
        integer(seq, 1, self.config.max_events)
        at = time.time_ns()
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            self.db.execute("SAVEPOINT semantic")
            reason = None
            try:
                self._message(participant.name, request["message"], seq, at)
            except (ValueError, TypeError, KeyError) as exc:
                self.db.execute("ROLLBACK TO semantic")
                reason = str(exc)
            self.db.execute("RELEASE semantic")
            event = {"protocol": 1, "channel": self.config.channel, "seq": seq,
                     "author": participant.name, "author_seq": author_seq,
                     "request_sha256": sha256_bytes(body), "at_ns": at,
                     "previous": last[1] if last else "0" * 64,
                     "accepted": reason is None, "reason": reason,
                     "message": request["message"] if reason is None else None}
            digest = sha256_bytes(canonical_json(event))
            encoded = canonical_json({"event": event, "sha256": digest})
            self.db.execute("INSERT INTO events(seq,author,author_seq,body,hash) VALUES (?,?,?,?,?)", (seq, participant.name, author_seq, encoded, digest))
            self.db.execute("INSERT OR REPLACE INTO streams VALUES (?,?)", (participant.name, author_seq))
        return True

    def _replay(self):
        for seq, body, digest in self.db.execute("SELECT seq,body,hash FROM events WHERE published=0 ORDER BY seq").fetchall():
            if decode_event(body, self.config, seq)["sha256"] != digest:
                raise Rejected("private journal corruption")
            for participant in self.config.participants:
                publish(event_file(participant.root, seq), body, participant.root)
            head = canonical_json({"channel": self.config.channel, "seq": seq, "sha256": digest})
            for participant in self.config.participants:
                atomic_write(participant.root / "feed/HEAD", head)
                if stable_read(participant.root / "feed/HEAD", 4096) != head:
                    raise Rejected("head readback mismatch")
                fsync_directory(participant.root / "feed")
                fsync_directory(participant.root)
            with self.db:
                self.db.execute("UPDATE events SET published=1 WHERE seq=?", (seq,))

    def once(self) -> dict:
        errors = []
        accepted = 0
        # A failed output stops all new intake, preventing unbounded fanout debt.
        self._replay()
        for participant in self.config.participants:
            try:
                changed = self._accept(participant)
            except (OSError, ValueError, RecursionError) as exc:
                errors.append({"participant": participant.name, "error": type(exc).__name__})
                continue
            accepted += changed
            self._replay()
        return {"processed": accepted, "errors": errors, **self.status()}

    def status(self) -> dict:
        now = time.time_ns()
        return {"channel": self.config.channel,
                "events": self.db.execute("SELECT count(*) FROM events").fetchone()[0],
                "pending": self.db.execute("SELECT count(*) FROM events WHERE published=0").fetchone()[0],
                "tasks": [dict(zip(("task", "assignee", "status", "revision"), row)) for row in self.db.execute("SELECT * FROM tasks ORDER BY id")],
                "acks": dict(self.db.execute("SELECT author,upto FROM acks")),
                "presence": [{"author": a, "session": s, "expires_ns": e, "online": now < e} for a, s, e in self.db.execute("SELECT * FROM presence")]}


class Client:
    """One private state directory per identity. Queued IDs survive crashes."""

    def __init__(self, config: ChannelConfig, author: str, state_dir: Path):
        self.config, self.author = config, author
        self.participant = config.participant(author)
        require_mailbox(config, self.participant)
        if not state_dir.is_absolute() or Path(os.path.abspath(state_dir)) != state_dir:
            raise ValueError("absolute normalized client state required")
        for root in (config.state_dir, *(p.root for p in config.participants)):
            if state_dir == root or state_dir in root.parents or root in state_dir.parents or (root.exists() and state_dir.exists() and os.path.samefile(root, state_dir)):
                raise ValueError("client state must be private and outside mailboxes/broker state")
        self.store = Store(state_dir, canonical_json({"manifest": object_json(config.manifest()), "author": author, "root": str(self.participant.root)}))
        self.db = self.store.db

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.store.close()

    def queue(self, message: dict, key: str) -> int:
        """Persist before publication; retry the same key and bytes after ambiguity."""
        name(key)
        digest = sha256_bytes(canonical_json(message))
        old = self.db.execute("SELECT seq,hash FROM sent WHERE id=?", (key,)).fetchone()
        if old:
            if old[1] != digest:
                raise Rejected("idempotency key conflicts with first message")
            return old[0]
        if self.db.execute("SELECT count(*) FROM outbox").fetchone()[0] >= self.config.max_pending:
            raise Rejected("client pending limit; sync existing IDs")
        row = self.db.execute("SELECT value FROM meta WHERE key='next'").fetchone()
        seq = int(row[0]) if row else 1
        integer(seq, 1, self.config.max_events)
        body = canonical_json({"protocol": 1, "channel": self.config.channel, "author": self.author, "seq": seq, "message": message})
        if len(body) > self.config.max_message_bytes:
            raise Rejected("message byte limit")
        # Strict JSON validation also rejects NaN/Infinity before persisting.
        object_json(body)
        with self.db:
            self.db.execute("INSERT INTO outbox VALUES (?,?)", (seq, body))
            self.db.execute("INSERT INTO sent VALUES (?,?,?)", (key, seq, digest))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES ('next',?)", (str(seq + 1),))
        return seq

    def sync(self) -> list[int]:
        published = []
        for seq, body in self.db.execute("SELECT seq,body FROM outbox ORDER BY seq").fetchall():
            request = object_json(body)
            expected = self.db.execute("SELECT hash FROM sent WHERE seq=?", (seq,)).fetchone()
            if request.get("channel") != self.config.channel or request.get("author") != self.author or request.get("seq") != seq or not expected or expected[0] != sha256_bytes(canonical_json(request.get("message"))):
                raise Rejected("outbox identity corruption")
            publish(self.participant.root / "inbox" / f"{seq:016d}.json", body, self.participant.root)
            with self.db:
                self.db.execute("DELETE FROM outbox WHERE seq=?", (seq,))
            published.append(seq)
        return published

    def pending(self) -> list[int]:
        return [row[0] for row in self.db.execute("SELECT seq FROM outbox ORDER BY seq")]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="administrator provisioning; does not configure ACLs")
    commands.add_parser("once")
    run = commands.add_parser("run")
    run.add_argument("--poll", type=float, default=0.2)
    commands.add_parser("status", help="private broker state; stop broker first")
    for action in ("send", "sync", "pending", "receive", "adapter"):
        command = commands.add_parser(action)
        command.add_argument("--participant", required=True)
        if action != "receive":
            command.add_argument("--state", type=Path, required=True)
        if action in {"receive", "adapter"}:
            command.add_argument("--after", type=int, default=0)
        if action == "send":
            command.add_argument("--id", required=True, help="stable idempotency key; reuse on retry")
            command.add_argument("--message", required=True, help="complete JSON message; not a command")
    args = parser.parse_args(argv)
    try:
        config = ChannelConfig.load(args.config)
        if args.command == "init":
            provision(config)
            print(json.dumps({"provisioned": config.channel, "acl_setup_required": True}))
        elif args.command in {"once", "run", "status"}:
            if args.command == "run" and not 0.01 <= args.poll <= 60:
                raise ValueError("poll must be finite and in 0.01..60 seconds")
            with Broker(config) as broker:
                if args.command == "status":
                    print(json.dumps(broker.status()))
                elif args.command == "once":
                    result = broker.once()
                    print(json.dumps(result))
                    return 1 if result["errors"] else 0
                else:
                    while True:
                        try:
                            result = broker.once()
                            # Bounded diagnostics, never message contents.
                            print(json.dumps({k: result[k] for k in ("events", "pending", "processed", "errors")}), flush=True)
                        except (OSError, ValueError, sqlite3.Error) as exc:
                            print(json.dumps({"degraded": type(exc).__name__}), file=sys.stderr, flush=True)
                        time.sleep(args.poll)
        elif args.command == "receive":
            for event in read_events(config, args.participant, args.after):
                print(json.dumps(event))
        else:
            with Client(config, args.participant, args.state) as client:
                if args.command == "send":
                    message = object_json(args.message.encode())
                    seq = client.queue(message, args.id)
                    # Emit the durable retry identity before transport can fail.
                    print(json.dumps({"channel": config.channel, "author": args.participant, "author_seq": seq, "state": "queued"}), flush=True)
                    client.sync()
                elif args.command == "sync":
                    print(json.dumps({"published": client.sync()}))
                elif args.command == "pending":
                    print(json.dumps({"pending": client.pending()}))
                else:
                    # One bounded JSON request/response for any external agent.
                    # No tools, shell, model credentials or executable selection.
                    raw = sys.stdin.buffer.read(config.max_message_bytes + 1)
                    if len(raw) > config.max_message_bytes:
                        raise Rejected("adapter input limit")
                    request = object_json(raw)
                    if request and set(request) != {"id", "message"}:
                        raise Rejected("adapter accepts {} or {id,message}")
                    # Recover pending work first; never implicitly retry effects.
                    client.sync()
                    queued = [client.queue(request["message"], request["id"])] if request else []
                    print(json.dumps({"queued": queued}), flush=True)
                    client.sync()
                    print(json.dumps({"events": read_events(config, args.participant, args.after)}))
        return 0
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError, TypeError, RecursionError, sqlite3.Error) as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
