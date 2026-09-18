"""Protocol-v2 store-and-forward transport. This module never executes commands.

The local journal and namespace ACLs are part of the trust boundary. Filesystem
hashes detect corruption, not a writer authorized to replace both data and hash.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import sqlite3
import stat
import sys
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from .latency import percentile
from .util import (
    atomic_write,
    canonical_json,
    check_path,
    fsync_directory,
    mkdir_durable,
    open_regular,
    parent_fd,
    safe_name,
    sha256_bytes,
)

if os.name == "nt":
    from .rally_windows import open_locked as open_regular


class Rejected(ValueError):
    """Untrusted, incomplete, conflicting, or over-budget publication."""


def component(value: str) -> str:
    # safe_name is shared with v2; explicitly exclude dot components here.
    if not isinstance(value, str) or value in {".", ".."}:
        raise Rejected("dot path component")
    return safe_name(value)


def relative(value: str) -> str:
    if not isinstance(value, str) or len(value) > 1024:
        raise Rejected("invalid relative path")
    parts = value.split("/")
    if len(parts) > 16:
        raise Rejected("path depth limit")
    for part in parts:
        component(part)
    return "/".join(parts)


@dataclass(frozen=True)
class Mapping:
    name: str
    sandbox: str
    outside: str
    direction: str = "protocol-v2"


@dataclass(frozen=True)
class RallyConfig:
    sandbox_root: Path
    outside_root: Path
    state_dir: Path
    targets: tuple[Mapping, ...]
    poll_min: float = 0.05
    poll_max: float = 1.0
    jitter: float = 0.2
    max_file_bytes: int = 16 * 1024 * 1024
    max_publication_bytes: int = 160 * 1024 * 1024
    max_files: int = 8500
    max_entries: int = 10000
    max_publications: int = 100000
    max_pending_bytes: int = 512 * 1024 * 1024
    sample_retention_seconds: float = 900
    fallback_overhead: float = 30
    backend: str = "python"

    def __post_init__(self) -> None:
        roots = [self.sandbox_root, self.outside_root, self.state_dir]
        for root in roots:
            if not root.is_absolute():
                raise ValueError("roots and state_dir must be absolute")
            check_path(root)
            if Path(os.path.abspath(root)) != root or ".." in root.parts:
                raise ValueError("roots must be normalized")
        for index, left in enumerate(roots):
            for right in roots[index + 1:]:
                if left == right or left in right.parents or right in left.parents:
                    raise ValueError("roots/state must be distinct and non-overlapping")
                if left.exists() and right.exists() and os.path.samefile(left, right):
                    raise ValueError("aliased roots")
        if not self.targets or self.backend != "python":
            raise ValueError("at least one mapping and backend=python required")
        for field in ("name", "sandbox", "outside"):
            values = [component(getattr(target, field)).casefold() for target in self.targets]
            if len(set(values)) != len(values):
                raise ValueError("duplicate/case-aliased target mapping")
        if any(target.direction != "protocol-v2" for target in self.targets):
            raise ValueError("direction must be protocol-v2 (fixed directional allowlist)")
        for field in ("poll_min", "poll_max", "sample_retention_seconds", "fallback_overhead"):
            value = getattr(self, field)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{field} must be finite and positive")
        if self.poll_max < self.poll_min or not 0 <= self.jitter <= 0.5:
            raise ValueError("invalid backoff/jitter")
        for field in ("max_file_bytes", "max_publication_bytes", "max_files", "max_entries", "max_publications", "max_pending_bytes"):
            if type(getattr(self, field)) is not int or getattr(self, field) <= 0:
                raise ValueError(f"{field} must be a positive integer")

    @classmethod
    def load(cls, path: Path) -> RallyConfig:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
        options = raw["rally"]
        targets = tuple(Mapping(**item) for item in raw["targets"])
        return cls(**{**options, **{key: Path(options[key]) for key in ("sandbox_root", "outside_root", "state_dir")}, "targets": targets})


def _signature(info: os.stat_result) -> tuple:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)


def stable_read(path: Path, limit: int, *, links: int = 1) -> bytes:
    """Bounded, nonblocking regular-file snapshot, with before/after identity.

    POSIX ancestors use O_NOFOLLOW descriptors; Windows source handles pin
    ancestors without delete sharing and deny writers while reading the leaf.
    """
    check_path(path)
    before = path.lstat()
    with open_regular(path) as handle:
        opened = os.fstat(handle.fileno())
        if _signature(before) != _signature(opened):
            raise Rejected("file replaced during open")
        if opened.st_nlink != links or opened.st_size > limit:
            raise Rejected("hard link or file size limit")
        if getattr(opened, "st_file_attributes", 0) & (0x400 | 0x200):
            raise Rejected("reparse or sparse file")
        if hasattr(opened, "st_blocks") and opened.st_size > opened.st_blocks * 512:
            raise Rejected("sparse file")
        body = handle.read(limit + 1)
        after = os.fstat(handle.fileno())
    check_path(path)
    if len(body) != opened.st_size or _signature(opened) != _signature(after) or _signature(after) != _signature(path.lstat()):
        raise Rejected("growing/replaced file")
    return body


def install_file(path: Path, body: bytes) -> None:
    """Recoverable atomic no-replace publication from journal-owned bytes.

    Only the deterministic staging name bound to these exact destination bytes
    may be cleaned up. An interrupted link is accepted only if both names refer
    to the same inode, its link count is exactly two, and its content matches.
    Destination directories must be writable only by the relay account.
    """
    check_path(path)
    mkdir_durable(path.parent)
    token = sha256_bytes(canonical_json([path.name, sha256_bytes(body)]))
    temporary = path.with_name(f".rally-{token}.tmp")
    check_path(temporary)
    with parent_fd(path) as directory:
        src = temporary if directory is None else temporary.name
        dst = path if directory is None else path.name
        try:
            info = temporary.lstat()
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISREG(info.st_mode):
                raise Rejected("unsafe recovery staging file")
            if info.st_nlink == 2:
                if not os.path.samefile(temporary, path) or stable_read(temporary, len(body), links=2) != body or stable_read(path, len(body), links=2) != body:
                    raise Rejected("ambiguous recovery hard link")
                os.unlink(src, dir_fd=directory)
                fsync_directory(path.parent)
                return
            if info.st_nlink != 1:
                raise Rejected("unexpected recovery hard link")
            # A crash during the staging write may leave partial bytes. This
            # exact journal-bound name is private to the single relay writer.
            os.unlink(src, dir_fd=directory)
        fd = os.open(src, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o660, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(src, dst, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
        finally:
            os.unlink(src, dir_fd=directory)
    fsync_directory(path.parent)


def object_json(body: bytes) -> dict:
    def pairs(items: list[tuple]) -> dict:
        result: dict = {}
        for key, value in items:
            if key in result:
                raise Rejected("duplicate JSON key")
            result[key] = value
        return result
    def constant(_: str) -> None:
        raise Rejected("non-finite JSON")
    def number(text: str) -> float:
        value = float(text)
        if not math.isfinite(value):
            raise Rejected("non-finite JSON number")
        return value
    value = json.loads(body, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)
    if not isinstance(value, dict):
        raise Rejected("expected complete JSON object")
    return value


def publication_digest(files: list[tuple[str, bytes]], config: RallyConfig) -> str:
    if not files or len(files) > config.max_files or sum(len(body) for _, body in files) > config.max_publication_bytes:
        raise Rejected("publication size/count limit")
    if any(len(body) > config.max_file_bytes for _, body in files):
        raise Rejected("publication file size limit")
    return sha256_bytes(canonical_json([(relative(path), sha256_bytes(body)) for path, body in files]))


class Snapshot:
    def __init__(self, root: Path, config: RallyConfig) -> None:
        self.root, self.config = root, config
        self.files: dict[str, bytes] = {}
        self.total = 0

    def read(self, name: str, limit: int | None = None) -> bytes:
        relative(name)
        if name not in self.files:
            if len(self.files) >= self.config.max_files:
                raise Rejected("publication file count limit")
            remaining = self.config.max_publication_bytes - self.total
            body = stable_read(self.root / name, min(self.config.max_file_bytes, remaining, limit if limit is not None else remaining))
            self.total += len(body)
            self.files[name] = body
        return self.files[name]

    def json(self, name: str, limit: int = 1024 * 1024) -> dict:
        return object_json(self.read(name, limit))

    def payload(self, name: str, entry: dict, limit: int | None = None) -> None:
        size, digest = entry["size"], entry["sha256"]
        if type(size) is not int or size < 0 or (limit is not None and size > limit) or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise Rejected("invalid payload framing")
        body = self.read(name, size)
        if len(body) != size or sha256_bytes(body) != digest:
            raise Rejected("payload checksum/length mismatch")

    def unchanged(self) -> None:
        for name, body in self.files.items():
            if stable_read(self.root / name, len(body)) != body:
                raise Rejected("publication changed during snapshot")

    def exact_tree(self, extra: set[str] | None = None, *, result: bool = False) -> None:
        allowed = set(self.files) | (extra or set())
        directories = {str(parent).replace("\\", "/") for name in allowed for parent in Path(name).parents if str(parent) != "."}
        if result:
            directories.update({"stdout", "stderr", "artifacts"})
        count = 0
        def walk(directory: Path, prefix: str = "") -> None:
            nonlocal count
            check_path(directory)
            with os.scandir(directory) as entries:
                for entry in entries:
                    count += 1
                    if count > self.config.max_entries:
                        raise Rejected("directory entry count limit")
                    name = relative(prefix + entry.name)
                    check_path(Path(entry.path))
                    # DirEntry.stat reports st_nlink=0 on Windows; lstat uses
                    # the actual file metadata and does not trust cached hints.
                    info = Path(entry.path).lstat()
                    # A failed artifact collection or watcher recovery can leave
                    # partial files and empty directories outside FINAL's manifest.
                    # Inspect their path/type bounds, but never copy their bytes.
                    artifact = result and name.startswith("artifacts/")
                    if stat.S_ISDIR(info.st_mode) and (name in directories or artifact):
                        walk(Path(entry.path), name + "/")
                    elif (name not in allowed and not artifact and not (result and re.fullmatch(r"(?:result-[0-9a-f]{32}\.json|(?:stdout|stderr)/[0-9]{8}\.chunk)", name))) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise Rejected("unknown or unsafe publication path")
        walk(self.root)


def request_snapshot(root: Path, config: RallyConfig, job: str) -> list[tuple[str, bytes]]:
    snap = Snapshot(root, config)
    commit = snap.json("COMMIT", 4096)
    manifest = snap.json("request.json")
    if commit.get("protocol") != 2 or manifest.get("protocol") != 2 or commit.get("job_id") != job or manifest.get("job_id") != job or sha256_bytes(snap.files["request.json"]) != commit.get("manifest_sha256"):
        raise Rejected("request identity/checksum mismatch")
    entries = manifest.get("uploads")
    if not isinstance(entries, list) or len(entries) > config.max_files - 2:
        raise Rejected("invalid upload list")
    names: set[str] = set()
    total = 0
    for entry in entries:
        name = component(entry["name"])
        if name.casefold() in names:
            raise Rejected("duplicate upload")
        names.add(name.casefold())
        snap.payload("uploads/" + name, entry)
        total += entry["size"]
    if type(commit.get("upload_bytes")) is not int or total != commit["upload_bytes"]:
        raise Rejected("upload total mismatch")
    snap.exact_tree()
    snap.unchanged()
    return [(name, body) for name, body in snap.files.items() if name != "COMMIT"] + [("COMMIT", snap.files["COMMIT"])]


def result_snapshot(root: Path, config: RallyConfig, job: str) -> list[tuple[str, bytes]]:
    snap = Snapshot(root, config)
    final = snap.json("FINAL", 4096)
    generation = component(final["result_file"])
    if not re.fullmatch(r"result-[0-9a-f]{32}\.json", generation):
        raise Rejected("invalid result generation name")
    result = snap.json(generation, config.max_file_bytes)
    if result.get("protocol") != 2 or result.get("job_id") != job or any(result.get(key) != final.get(key) for key in ("job_id", "watcher_id", "fence")) or sha256_bytes(snap.files[generation]) != final.get("result_sha256"):
        raise Rejected("result identity/checksum mismatch")
    for key in ("watcher_id", "fence"):
        component(result[key])
    streams = result.get("streams")
    if not isinstance(streams, dict) or set(streams) != {"stdout", "stderr"}:
        raise Rejected("invalid stream manifest")
    for stream, entries in streams.items():
        count = result.get(stream + "_chunks")
        if type(count) is not int or not 0 <= count <= 4096 or not isinstance(entries, list) or len(entries) != count:
            raise Rejected("invalid chunk count")
        for index, entry in enumerate(entries):
            snap.payload(f"{stream}/{index:08d}.chunk", entry, 65536)
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) > config.max_files:
        raise Rejected("invalid artifact manifest")
    names: set[str] = set()
    for entry in artifacts:
        name = relative(entry["name"])
        if name.casefold() in names:
            raise Rejected("duplicate artifact")
        names.add(name.casefold())
        snap.payload("artifacts/" + name, entry)
    # CLAIMED/RUNNING are independent, immutable status publications. A recovery
    # FINAL may have a new fence and omit provisional chunks from the old writer.
    # Do not copy any unreferenced file, including old generations/chunks.
    snap.exact_tree({"CLAIMED", "RUNNING"}, result=True)
    snap.unchanged()
    return [(name, body) for name, body in snap.files.items() if name != "FINAL"] + [("FINAL", snap.files["FINAL"])]


class Rally:
    def __init__(self, config: RallyConfig, *, dry_run: bool = False) -> None:
        self.config, self.dry_run = config, dry_run
        self.errors = 0
        self.changed = 0
        self.db: sqlite3.Connection | None = None
        self._lock: Any = None

    def __enter__(self) -> Self:
        config = self.config
        roots: list[Path] = []
        for mapping in config.targets:
            for root in (config.sandbox_root / mapping.sandbox, config.outside_root / mapping.outside):
                roots.append(root)
                for name in ("inbox", "results", "control/cancel", "health/pings", "health/pongs"):
                    check_path(root / name)
                    if not (root / name).is_dir():
                        raise ValueError("owner must provision both target namespaces first")
        if any(os.path.samefile(left, right) for index, left in enumerate(roots) for right in roots[index + 1:]):
            raise Rejected("aliased target roots")
        if not self.dry_run:
            config.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            check_path(config.state_dir)
            for name in ("rally.lock", "rally.sqlite3", "rally.sqlite3-journal"):
                path = config.state_dir / name
                check_path(path)
                if path.exists() and (not stat.S_ISREG(path.lstat().st_mode) or path.lstat().st_nlink != 1):
                    raise Rejected("unsafe state file")
            self._lock = (config.state_dir / "rally.lock").open("a+b")
            try:
                if os.name == "nt":
                    import msvcrt
                    self._lock.write(b"0")
                    self._lock.flush()
                    self._lock.seek(0)
                    msvcrt.locking(self._lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                self._lock.close()
                raise Rejected("another rally owns this local state") from None
        try:
            self.db = sqlite3.connect(":memory:" if self.dry_run else config.state_dir / "rally.sqlite3")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS known (target TEXT, kind TEXT, name TEXT, first REAL, PRIMARY KEY(target,kind,name));
                CREATE TABLE IF NOT EXISTS publications (target TEXT, direction TEXT, key TEXT, digest TEXT, done INTEGER DEFAULT 0, first REAL, PRIMARY KEY(target,direction,key));
                CREATE TABLE IF NOT EXISTS files (target TEXT, direction TEXT, key TEXT, ordinal INTEGER, path TEXT, body BLOB, PRIMARY KEY(target,direction,key,ordinal));
                CREATE TABLE IF NOT EXISTS samples (at REAL, kind TEXT, seconds REAL);
            """)
            identity = canonical_json({"schema": 1, "sandbox": str(config.sandbox_root), "outside": str(config.outside_root), "targets": [vars(mapping) for mapping in config.targets]}).decode()
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO meta VALUES ('identity', ?)", (identity,))
            if self.db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()[0] != identity:
                raise Rejected("journal belongs to a different mapping; use a fresh namespace and state")
            if self.db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise Rejected("journal integrity check failed")
            if not self.dry_run:
                fsync_directory(config.state_dir)
                fsync_directory(config.state_dir.parent)
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *args: object) -> None:
        if self.db:
            self.db.close()
        if self._lock:
            self._lock.close()

    def log(self, event: str, **fields: Any) -> None:
        print(canonical_json({"at_ns": time.time_ns(), "event": event, **fields}).decode(), file=sys.stderr, flush=True)

    def attempt(self, action: Any, *args: Any) -> None:
        try:
            action(*args)
        except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
            self.errors += 1
            # No file contents or command arguments enter diagnostics.
            self.log("deferred", operation=action.__name__, target=args[0].name if args and isinstance(args[0], Mapping) else None,
                     identity=str(args[-1]) if args and isinstance(args[-1], str) else None,
                     error_type=type(exc).__name__, reason=str(exc) if isinstance(exc, Rejected) else "unavailable or invalid protocol data")

    def remember(self, target: str, kind: str, name: str) -> None:
        component(name)
        assert self.db is not None
        if self.db.execute("SELECT 1 FROM known WHERE target=? AND kind=? AND name=?", (target, kind, name)).fetchone():
            return
        if self.db.execute("SELECT count(*) FROM known").fetchone()[0] >= self.config.max_entries:
            raise Rejected("tracked identity limit; retain state and rotate only drained namespaces")
        with self.db:
            self.db.execute("INSERT INTO known VALUES (?,?,?,?)", (target, kind, name, time.time()))

    def discover(self, mapping: Mapping, root: Path, kind: str, suffix: str) -> None:
        check_path(root)
        with os.scandir(root) as entries:
            for index, entry in enumerate(entries):
                if index >= self.config.max_entries:
                    raise Rejected("discovery count limit")
                # Only generated staging patterns are unpublished. A cancellation
                # is named by its job ID, which may legally end in .tmp/.staging.
                if kind == "job" and re.fullmatch(r".+\.[0-9a-f]{32}\.staging", entry.name):
                    continue
                if kind == "probe" and re.fullmatch(r"p-[0-9a-f]{32}\.staging", entry.name):
                    continue
                if kind == "cancel" and re.fullmatch(r"\..+\.[0-9]+\.[0-9a-f]{8}\.tmp", entry.name):
                    try:
                        value = object_json(stable_read(Path(entry.path), min(65536, self.config.max_file_bytes)))
                    except (OSError, ValueError, RecursionError):
                        continue
                    # Even a temp-looking name is a valid cancellation ID if its
                    # complete record identifies that exact name, not another ID.
                    if value.get("job_id") != entry.name:
                        continue
                if suffix and not entry.name.endswith(suffix):
                    raise Rejected("unknown ingress path")
                name = entry.name[:-len(suffix)] if suffix else entry.name
                self.remember(mapping.name, kind, name)

    def publish(self, mapping: Mapping, direction: str, key: str, files: list[tuple[str, bytes]], first: float | None = None) -> None:
        assert self.db is not None
        digest = publication_digest(files, self.config)
        identity = (mapping.name, direction, key)
        previous = self.db.execute("SELECT digest,done FROM publications WHERE target=? AND direction=? AND key=?", identity).fetchone()
        if previous:
            if previous[0] != digest:
                raise Rejected("first-valid-publication conflict; operator inspection required")
            if previous[1]:
                return
        elif self.dry_run:
            self.log("would_publish", target=mapping.name, direction=direction, key=key, files=len(files), bytes=sum(len(body) for _, body in files))
            return
        else:
            if self.db.execute("SELECT count(*) FROM publications").fetchone()[0] >= self.config.max_publications:
                raise Rejected("journal publication retention limit")
            pending = self.db.execute("SELECT coalesce(sum(length(body)),0) FROM files").fetchone()[0]
            if pending + sum(len(body) for _, body in files) > self.config.max_pending_bytes:
                raise Rejected("pending journal byte limit")
            # FULL synchronous transaction commits the complete validated bytes
            # before the first destination side effect, including on restart.
            with self.db:
                self.db.execute("INSERT INTO publications VALUES (?,?,?,?,0,?)", (*identity, digest, first if first is not None else time.time()))
                self.db.executemany("INSERT INTO files VALUES (?,?,?,?,?,?)", [(*identity, index, path, body) for index, (path, body) in enumerate(files)])
        self.replay(mapping, direction, key)

    def replay(self, mapping: Mapping, direction: str, key: str) -> None:
        assert self.db is not None
        identity = (mapping.name, direction, key)
        root = self.config.outside_root / mapping.outside if direction == "outbound" else self.config.sandbox_root / mapping.sandbox
        rows = self.db.execute("SELECT path,body FROM files WHERE target=? AND direction=? AND key=? ORDER BY ordinal", identity).fetchall()
        expected, first = self.db.execute("SELECT digest,first FROM publications WHERE target=? AND direction=? AND key=?", identity).fetchone()
        if publication_digest(rows, self.config) != expected:
            raise Rejected("journal payload checksum mismatch; preserve state for inspection")
        for name, body in rows:
            path = root / relative(name)
            try:
                install_file(path, body)
            except FileExistsError:
                if stable_read(path, len(body)) != body:
                    raise Rejected("destination conflict; refusing replacement")
            # Read back every dependency before advancing to its marker.
            if stable_read(path, len(body)) != body:
                raise Rejected("destination verification deferred")
            # Existing equal files may be remnants of a failed directory fsync.
            # Retry all namespace directory barriers, including mkdir ancestors,
            # before advancing to another file/marker or committing journal done.
            for directory in (path.parent, *path.parent.parents):
                fsync_directory(directory)
                if directory == root:
                    break
        with self.db:
            self.db.execute("UPDATE publications SET done=1 WHERE target=? AND direction=? AND key=?", identity)
            self.db.execute("DELETE FROM files WHERE target=? AND direction=? AND key=?", identity)
            self.db.execute("INSERT INTO samples VALUES (?,?,?)", (time.time(), direction, max(0, time.time() - first)))
        self.changed += 1
        self.log("published", target=mapping.name, direction=direction, key=key, files=len(rows))

    def done(self, mapping: Mapping, direction: str, key: str) -> bool:
        assert self.db is not None
        return bool(self.db.execute("SELECT 1 FROM publications WHERE target=? AND direction=? AND key=? AND done=1", (mapping.name, direction, key)).fetchone())

    def request(self, mapping: Mapping, job: str) -> None:
        started = time.time()
        prefix = f"inbox/{job}.ready/"
        files = request_snapshot(self.config.sandbox_root / mapping.sandbox / prefix, self.config, job)
        self.publish(mapping, "outbound", prefix, [(prefix + name, body) for name, body in files], started)

    def result(self, mapping: Mapping, job: str) -> None:
        started = time.time()
        prefix = f"results/{job}/"
        files = result_snapshot(self.config.outside_root / mapping.outside / prefix, self.config, job)
        self.publish(mapping, "inbound", prefix, [(prefix + name, body) for name, body in files], started)

    def record(self, mapping: Mapping, direction: str, path: str, name: str, kind: str) -> None:
        started = time.time()
        root = self.config.sandbox_root / mapping.sandbox if direction == "outbound" else self.config.outside_root / mapping.outside
        body = stable_read(root / path, min(65536, self.config.max_file_bytes))
        value = object_json(body)
        if value.get("probe_id" if kind in {"ping", "pong"} else "job_id") != name:
            raise Rejected("record identity mismatch")
        for field in {"cancel": ("at_ns",), "ping": ("client_ns",), "pong": ("client_ns", "observed_ns", "responded_ns"), "status": ()}[kind]:
            if type(value.get(field)) is not int or value[field] < 0:
                raise Rejected("invalid record timestamp")
        if kind in {"status", "pong"}:
            component(value["watcher_id"])
        if kind == "status":
            component(value["fence"])
        if kind == "ping":
            snap = Snapshot((root / path).parent, self.config)
            snap.files["PING"] = body
            snap.exact_tree()
        if kind == "pong":
            ping_path = f"health/pings/{name}.ready/PING"
            ping = object_json(stable_read(self.config.sandbox_root / mapping.sandbox / ping_path, 65536))
            if value["client_ns"] != ping["client_ns"]:
                raise Rejected("pong does not match ping")
        if stable_read(root / path, len(body)) != body:
            raise Rejected("record changed during validation")
        was_done = self.done(mapping, direction, path)
        self.publish(mapping, direction, path, [(path, body)], started)
        if kind == "pong" and not was_done and not self.dry_run:
            assert self.db is not None
            first = self.db.execute("SELECT first FROM known WHERE target=? AND kind='probe' AND name=?", (mapping.name, name)).fetchone()[0]
            with self.db:
                self.db.execute("INSERT INTO samples VALUES (?,?,?)", (time.time(), "roundtrip", max(0, time.time() - first)))

    def heartbeat(self, mapping: Mapping) -> None:
        source = self.config.outside_root / mapping.outside / "health/heartbeat.json"
        body = stable_read(source, min(65536, self.config.max_file_bytes))
        value = object_json(body)
        if type(value.get("at_ns")) is not int or value["at_ns"] < 0 or value.get("state") not in {"READY", "BUSY", "STOPPED"}:
            raise Rejected("invalid watcher heartbeat")
        component(value["watcher_id"])
        if not self.dry_run:
            destination = self.config.sandbox_root / mapping.sandbox / "health/heartbeat.json"
            # Do not regress a heartbeat on a stale read (including restart).
            assert self.db is not None
            key = "heartbeat:" + mapping.name
            previous = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            if previous and value["at_ns"] <= int(previous[0]):
                return
            atomic_write(destination, body)
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value["at_ns"])))

    def status(self) -> dict:
        assert self.db is not None
        now = time.time()
        metrics: dict = {}
        for kind in ("outbound", "inbound", "roundtrip"):
            values = [row[0] for row in self.db.execute("SELECT seconds FROM samples WHERE kind=? AND at>? ORDER BY at DESC LIMIT 200", (kind, now - self.config.sample_retention_seconds))]
            metrics[kind] = {"samples": len(values), **{f"p{p}": percentile(values, p / 100) for p in (50, 95, 99)}}
        overhead = self.config.fallback_overhead
        if metrics["roundtrip"]["samples"] >= 3:
            overhead = max(overhead, metrics["roundtrip"]["p99"] + 2 * self.config.poll_max + 4)
        return {"protocol": 2, "at_ns": time.time_ns(), "state": "DEGRADED" if self.errors else "READY", "errors": self.errors, "dry_run": self.dry_run,
                "pending": self.db.execute("SELECT count(*) FROM publications WHERE done=0").fetchone()[0],
                "retained_publications": self.db.execute("SELECT count(*) FROM publications").fetchone()[0],
                "metrics": metrics, "measurement_scope": "relay-observed forwarding legs and ping-to-pong; not client end-to-end", "recommended_overhead": overhead}

    def once(self) -> dict:
        assert self.db is not None
        self.errors = self.changed = 0
        for mapping in self.config.targets:
            for direction, key in self.db.execute("SELECT direction,key FROM publications WHERE target=? AND done=0", (mapping.name,)).fetchall():
                if not self.dry_run:
                    self.attempt(self.replay, mapping, direction, key)
            drop = self.config.sandbox_root / mapping.sandbox
            for path, kind, suffix in (("inbox", "job", ".ready"), ("control/cancel", "cancel", ""), ("health/pings", "probe", ".ready")):
                self.attempt(self.discover, mapping, drop / path, kind, suffix)
            # Discovery is only a hint. Previously seen identities are polled
            # at exact paths forever, even if later listings omit them.
            for kind, name in self.db.execute("SELECT kind,name FROM known WHERE target=? ORDER BY first", (mapping.name,)).fetchall():
                if kind == "job":
                    self.attempt(self.request, mapping, name)
                    for marker in ("CLAIMED", "RUNNING"):
                        self.optional_record(mapping, f"results/{name}/{marker}", name)
                    # Missing exact result paths are normal, not proof of loss.
                    self.optional_result(mapping, name)
                elif kind == "cancel":
                    self.attempt(self.record, mapping, "outbound", f"control/cancel/{name}", name, "cancel")
                else:
                    self.attempt(self.record, mapping, "outbound", f"health/pings/{name}.ready/PING", name, "ping")
                    try:
                        self.record(mapping, "inbound", f"health/pongs/{name}", name, "pong")
                    except FileNotFoundError:
                        pass
                    except (OSError, ValueError, KeyError, TypeError) as exc:
                        self.errors += 1
                        self.log("deferred", operation="pong", error_type=type(exc).__name__)
            self.attempt(self.heartbeat, mapping)
        if not self.dry_run:
            with self.db:
                self.db.execute("DELETE FROM samples WHERE at<?", (time.time() - self.config.sample_retention_seconds,))
                self.db.execute("DELETE FROM samples WHERE rowid NOT IN (SELECT rowid FROM samples ORDER BY at DESC LIMIT 600)")
        health = self.status()
        if not self.dry_run:
            for mapping in self.config.targets:
                self.attempt(atomic_write, self.config.sandbox_root / mapping.sandbox / "health/rally.json", canonical_json(health))
            atomic_write(self.config.state_dir / "health.json", canonical_json(self.status()))
        return self.status()

    def optional_record(self, mapping: Mapping, path: str, job: str) -> None:
        try:
            self.record(mapping, "inbound", path, job, "status")
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self.errors += 1
            self.log("deferred", operation="status", target=mapping.name, identity=path, error_type=type(exc).__name__)

    def optional_result(self, mapping: Mapping, job: str) -> None:
        try:
            self.result(mapping, job)
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
            self.errors += 1
            self.log("deferred", operation="result", target=mapping.name, identity=job, error_type=type(exc).__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fs-rally", description="Directional protocol-v2 sidecar; never executes commands")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("mode", choices=("run", "once", "dry-run", "status", "health"), default="run", nargs="?")
    args = parser.parse_args(argv)
    try:
        config = RallyConfig.load(args.config)
        if args.mode in {"status", "health"}:
            health = object_json(stable_read(config.state_dir / "health.json", 65536))
            age = (time.time_ns() - health["at_ns"]) / 1e9
            health["age_seconds"] = age
            healthy = 0 <= age <= max(15, config.poll_max * 4) and health["state"] == "READY" and health["pending"] == 0
            print(json.dumps(health, sort_keys=True))
            return 0 if healthy else 1
        with Rally(config, dry_run=args.mode == "dry-run") as rally:
            delay = config.poll_min
            while True:
                health = rally.once()
                if args.mode != "run":
                    print(json.dumps(health, sort_keys=True))
                    return 1 if health["errors"] or health["pending"] else 0
                delay = config.poll_min if rally.changed else min(config.poll_max, delay * 1.5)
                time.sleep(delay * random.uniform(1 - config.jitter, 1 + config.jitter))
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        print(canonical_json({"event": "fatal", "error_type": type(exc).__name__, "reason": str(exc)}).decode(), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
