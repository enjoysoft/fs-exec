from __future__ import annotations

import errno
import hashlib
import json
import os
import random
import stat
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


def now_ns() -> int:
    return time.time_ns()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open_regular(path) as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def hash_bounded(path: Path, limit: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with open_regular(path) as handle:
        if os.fstat(handle.fileno()).st_size > limit:
            raise ValueError("file exceeds byte limit")
        while True:
            block = handle.read(min(65536, limit - size + 1))
            if not block:
                return size, digest.hexdigest()
            size += len(block)
            if size > limit:
                raise ValueError("file exceeds byte limit")
            digest.update(block)


def mkdir_durable(path: Path) -> None:
    """Preserve inherited directional ACLs and persist each new parent entry."""
    check_path(path)
    if path.is_dir():
        return
    mkdir_durable(path.parent)
    try:
        path.mkdir(mode=0o770)
    except FileExistsError:
        check_path(path)
        if not path.is_dir():
            raise
    fsync_directory(path.parent)


def check_path(path: Path) -> None:
    """Reject existing symlinks and Windows reparse points, including parents."""
    path = Path(os.path.abspath(path))
    for part in (*reversed(path.parents), path):
        try:
            info = part.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("symlink/reparse point is not permitted")


@contextmanager
def parent_fd(path: Path):
    """Pin POSIX path components; Windows checks are best effort, not race-proof."""
    path = Path(os.path.abspath(path))
    check_path(path)
    if os.name == "nt":
        yield None
        return
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


@contextmanager
def open_regular(path: Path):
    with parent_fd(path) as directory:
        fd = os.open(path if directory is None else path.name,
                     os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0), dir_fd=directory)
    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("expected regular file")
        yield handle


def read_bounded(path: Path, limit: int) -> bytes:
    with open_regular(path) as handle:
        if os.fstat(handle.fileno()).st_size > limit:
            raise ValueError("file exceeds byte limit")
        body = handle.read(limit + 1)
        if len(body) > limit:
            raise ValueError("file exceeds byte limit")
        return body


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return  # Python does not expose a portable directory flush on Windows.
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if exc.errno not in {errno.EINVAL, errno.ENOTSUP}:
                raise
    finally:
        os.close(fd)


def copy_bounded(source: Path, destination: Path, limit: int, *, shared: bool = False) -> tuple[int, str]:
    """Copy a regular file without following links or exceeding the byte budget."""
    digest = hashlib.sha256()
    size = 0
    check_path(destination)
    mkdir_durable(destination.parent)
    with open_regular(source) as src, parent_fd(destination) as directory:
        if os.fstat(src.fileno()).st_size > limit:
            raise ValueError("file exceeds byte limit")
        fd = os.open(destination if directory is None else destination.name,
                     os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o660 if shared else 0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as dst:
                while True:
                    block = src.read(min(65536, limit - size + 1))
                    if not block:
                        break
                    if size + len(block) > limit:
                        raise ValueError("file exceeds byte limit")
                    dst.write(block)
                    digest.update(block)
                    size += len(block)
                dst.flush()
                os.fsync(dst.fileno())
        except BaseException:
            os.unlink(destination if directory is None else destination.name, dir_fd=directory)
            raise
    fsync_directory(destination.parent)
    return size, digest.hexdigest()


def write_exclusive(path: Path, data: bytes) -> None:
    _publish(path, data, exclusive=True)


def atomic_write(path: Path, data: bytes) -> None:
    _publish(path, data, exclusive=False)


def _publish(path: Path, data: bytes, *, exclusive: bool) -> None:
    check_path(path)
    mkdir_durable(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{random.randrange(1 << 32):08x}.tmp")
    with parent_fd(path) as directory:
        src = temporary if directory is None else temporary.name
        dst = path if directory is None else path.name
        # Group-class bits are the POSIX ACL mask, not a grant to every group.
        # Parent default ACLs must grant readers r-- and writers rw- separately.
        fd = os.open(src, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o660, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            if exclusive:
                # Atomic no-replace publication of already complete bytes. This
                # requires hard-link support; fail closed on unsupported shares.
                os.link(src, dst, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            else:
                os.replace(src, dst, src_dir_fd=directory, dst_dir_fd=directory)
        finally:
            try:
                os.unlink(src, dir_fd=directory)
            except FileNotFoundError:
                pass
    fsync_directory(path.parent)


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(read_bounded(path, 16 * 1024 * 1024))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    return value


def exact_wait(path: Path, timeout: float, *, initial: float = 0.03, maximum: float = 0.5) -> bool:
    """Poll an exact path; directory listings are deliberately not trusted."""
    deadline = time.monotonic() + max(timeout, 0)
    delay = initial
    while True:
        try:
            if path.exists():
                return True
        except OSError:
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(remaining, delay * random.uniform(0.8, 1.2)))
        delay = min(maximum, delay * 1.5)


def safe_name(value: str) -> str:
    reserved = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
    if not isinstance(value, str) or not value or len(value) > 200 or value.endswith(".") or value.split(".")[0].upper() in reserved or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in value):
        raise ValueError(f"unsafe name: {value!r}")
    return value


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = canonical_json(value) + b"\n"
    with path.open("ab") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def tail_jsonl(path: Path, limit: int = 200) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    lines = path.read_bytes().splitlines()[-limit:]
    for line in lines:
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                yield value
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
