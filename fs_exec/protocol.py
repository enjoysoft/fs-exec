from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Policy
from .util import (
    canonical_json,
    check_path,
    copy_bounded,
    fsync_directory,
    hash_bounded,
    mkdir_durable,
    now_ns,
    read_bounded,
    safe_name,
    sha256_bytes,
    write_exclusive,
)

PROTOCOL_VERSION = 2


class ProtocolError(RuntimeError):
    pass


@dataclass(frozen=True)
class TargetPaths:
    root: Path

    @property
    def inbox(self) -> Path:
        return self.root / "inbox"

    @property
    def claims(self) -> Path:
        return self.root / "claims"

    @property
    def results(self) -> Path:
        return self.root / "results"

    @property
    def control(self) -> Path:
        return self.root / "control"

    @property
    def health(self) -> Path:
        return self.root / "health"

    def initialize(self) -> None:
        """Owner-only provisioning. Clients must not create server namespaces."""
        for path in (self.inbox, self.claims, self.results, self.control / "cancel", self.health / "pings", self.health / "pongs"):
            mkdir_durable(path)
            fsync_directory(path)
            fsync_directory(path.parent)
        fsync_directory(self.root.parent)

    def require_client_namespace(self) -> None:
        for path in (self.inbox, self.control / "cancel", self.health / "pings"):
            check_path(path)
            if not path.is_dir():
                raise ProtocolError("target namespaces must be provisioned by the owner first")

    def ready(self, job_id: str) -> Path:
        return self.inbox / f"{safe_name(job_id)}.ready"

    def result(self, job_id: str) -> Path:
        return self.results / safe_name(job_id)


def new_job_id() -> str:
    return f"j-{uuid.uuid4().hex}"


def publish_request(paths: TargetPaths, manifest: dict[str, Any], uploads: list[tuple[Path, str]] | None = None) -> str:
    paths.require_client_namespace()
    job_id = safe_name(str(manifest.get("job_id") or new_job_id()))
    manifest = dict(manifest)
    manifest.update({"job_id": job_id, "protocol": PROTOCOL_VERSION, "published_ns": now_ns()})
    staging = paths.inbox / f"{job_id}.{uuid.uuid4().hex}.staging"
    ready = paths.ready(job_id)
    if ready.exists():
        raise ProtocolError(f"job already exists: {job_id}")
    staging.mkdir(mode=0o770)
    try:
        upload_entries: list[dict[str, Any]] = []
        upload_names: set[str] = set()
        total = 0
        for source, remote_name in uploads or []:
            remote_name = safe_name(remote_name)
            if remote_name in upload_names:
                raise ProtocolError(f"duplicate upload name: {remote_name}")
            upload_names.add(remote_name)
            destination = staging / "uploads" / remote_name
            size, digest = copy_bounded(source, destination, 128 * 1024 * 1024 - total, shared=True)
            total += size
            upload_entries.append({"name": remote_name, "size": size, "sha256": digest})
        manifest["uploads"] = upload_entries
        body = canonical_json(manifest)
        request_path = staging / "request.json"
        write_exclusive(request_path, body)
        commit = canonical_json({"protocol": PROTOCOL_VERSION, "job_id": job_id, "manifest_sha256": sha256_bytes(body), "upload_bytes": total})
        write_exclusive(staging / "COMMIT", commit)
        fsync_directory(staging)
        os.replace(staging, ready)
        fsync_directory(paths.inbox)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return job_id


def verify_request(job_dir: Path, visibility_timeout: float = 30, *, policy: Policy | None = None) -> dict[str, Any]:
    policy = policy or Policy()
    deadline = time.monotonic() + visibility_timeout
    while True:
        try:
            return _verify_request(job_dir, policy)
        except (OSError, ProtocolError, json.JSONDecodeError, UnicodeDecodeError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))


def _verify_request(job_dir: Path, policy: Policy) -> dict[str, Any]:
    commit = json.loads(read_bounded(job_dir / "COMMIT", 4096))
    body = read_bounded(job_dir / "request.json", policy.max_request_bytes)
    if sha256_bytes(body) != commit.get("manifest_sha256"):
        raise ProtocolError("request manifest checksum mismatch")
    manifest = json.loads(body)
    if manifest.get("protocol") != PROTOCOL_VERSION or commit.get("protocol") != PROTOCOL_VERSION or manifest.get("job_id") != commit.get("job_id") or manifest.get("job_id") != job_dir.name.removesuffix(".ready"):
        raise ValueError("request identity or protocol mismatch")
    uploads = manifest.get("uploads", [])
    if not isinstance(uploads, list) or len(uploads) > policy.max_upload_files:
        raise ValueError("upload count exceeds policy limit")
    names: set[str] = set()
    total = 0
    for upload in uploads:
        name = safe_name(upload["name"])
        size = upload["size"]
        if name.lower() in names or type(size) is not int or size < 0:
            raise ValueError("invalid or duplicate upload entry")
        names.add(name.lower())
        total += size
        if total > policy.max_upload_bytes:
            raise ValueError("uploaded data exceeds policy limit")
    if total != commit.get("upload_bytes"):
        raise ValueError("upload total mismatch")
    # All limits are checked before consuming any upload bytes.
    for upload in uploads:
        name = upload["name"]
        path = job_dir / "uploads" / name
        size, digest = hash_bounded(path, upload["size"])
        if size != upload["size"] or digest != upload["sha256"]:
            raise ProtocolError(f"upload checksum mismatch: {name}")
    return manifest


def publish_cancel(paths: TargetPaths, job_id: str) -> None:
    paths.require_client_namespace()
    try:
        write_exclusive(paths.control / "cancel" / safe_name(job_id), canonical_json({"job_id": job_id, "at_ns": now_ns()}))
    except FileExistsError:
        pass


def publish_final(result_dir: Path, result: dict[str, Any]) -> None:
    mkdir_durable(result_dir)
    if (result_dir / "FINAL").exists():
        raise FileExistsError("FINAL already published")
    generation = f"result-{uuid.uuid4().hex}.json"
    body = canonical_json(result)
    write_exclusive(result_dir / generation, body)
    write_exclusive(result_dir / "FINAL", canonical_json({"result_file": generation, "job_id": result["job_id"], "watcher_id": result.get("watcher_id"), "fence": result.get("fence"), "result_sha256": sha256_bytes(body), "at_ns": now_ns()}))


def read_final(result_dir: Path) -> dict[str, Any]:
    final = json.loads(read_bounded(result_dir / "FINAL", 4096))
    if not isinstance(final, dict):
        raise ProtocolError("invalid FINAL pointer")
    generation = safe_name(final["result_file"])
    body = read_bounded(result_dir / generation, 16 * 1024 * 1024)
    if sha256_bytes(body) != final.get("result_sha256"):
        raise ProtocolError("result checksum mismatch")
    result = json.loads(body)
    if not isinstance(result, dict):
        raise ProtocolError("invalid result object")
    if any(result.get(key) != final.get(key) for key in ("job_id", "watcher_id", "fence")) or result.get("job_id") != result_dir.name:
        raise ProtocolError("result identity mismatch")
    streams = result.get("streams", {})
    if not isinstance(streams, dict):
        raise ProtocolError("invalid streams")
    for name in ("stdout", "stderr"):
        count = result.get(f"{name}_chunks", 0)
        entries = streams.get(name, [])
        if type(count) is not int or not 0 <= count <= 4096 or not isinstance(entries, list) or len(entries) != count:
            raise ProtocolError("invalid stream manifest")
        for entry in entries:
            if not isinstance(entry, dict) or type(entry.get("size")) is not int or not 0 <= entry["size"] <= 65536 or not isinstance(entry.get("sha256"), str) or len(entry["sha256"]) != 64:
                raise ProtocolError("invalid stream entry")
    return result
