from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import atomic_write, canonical_json, exact_wait, now_ns, read_json, safe_name, sha256_bytes, sha256_file, write_exclusive

PROTOCOL_VERSION = 1


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
        for path in (self.inbox, self.claims, self.results, self.control / "cancel", self.health / "pings", self.health / "pongs"):
            path.mkdir(parents=True, exist_ok=True)

    def ready(self, job_id: str) -> Path:
        return self.inbox / f"{safe_name(job_id)}.ready"

    def result(self, job_id: str) -> Path:
        return self.results / safe_name(job_id)


def new_job_id() -> str:
    return f"j-{uuid.uuid4().hex}"


def publish_request(paths: TargetPaths, manifest: dict[str, Any], uploads: list[tuple[Path, str]] | None = None) -> str:
    paths.initialize()
    job_id = safe_name(str(manifest.get("job_id") or new_job_id()))
    manifest = dict(manifest)
    manifest.update({"job_id": job_id, "protocol": PROTOCOL_VERSION, "published_ns": now_ns()})
    staging = paths.inbox / f"{job_id}.{uuid.uuid4().hex}.staging"
    ready = paths.ready(job_id)
    if ready.exists():
        raise ProtocolError(f"job already exists: {job_id}")
    staging.mkdir(mode=0o700)
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
            destination.parent.mkdir(exist_ok=True)
            shutil.copyfile(source, destination)
            size = destination.stat().st_size
            total += size
            upload_entries.append({"name": remote_name, "size": size, "sha256": sha256_file(destination)})
        manifest["uploads"] = upload_entries
        body = canonical_json(manifest)
        request_path = staging / "request.json"
        with request_path.open("wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        commit = canonical_json({"protocol": PROTOCOL_VERSION, "job_id": job_id, "manifest_sha256": sha256_bytes(body), "upload_bytes": total})
        write_exclusive(staging / "COMMIT", commit)
        os.replace(staging, ready)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return job_id


def verify_request(job_dir: Path, visibility_timeout: float = 30) -> dict[str, Any]:
    if not exact_wait(job_dir / "request.json", visibility_timeout):
        raise ProtocolError("request manifest was not visible before transport timeout")
    commit = read_json(job_dir / "COMMIT")
    body = (job_dir / "request.json").read_bytes()
    if sha256_bytes(body) != commit.get("manifest_sha256"):
        raise ProtocolError("request manifest checksum mismatch")
    manifest = read_json(job_dir / "request.json")
    if manifest.get("protocol") != PROTOCOL_VERSION or manifest.get("job_id") != commit.get("job_id"):
        raise ProtocolError("request identity or protocol mismatch")
    for upload in manifest.get("uploads", []):
        name = safe_name(str(upload["name"]))
        path = job_dir / "uploads" / name
        if not exact_wait(path, visibility_timeout):
            raise ProtocolError(f"upload was not visible before transport timeout: {name}")
        if path.stat().st_size != int(upload["size"]) or sha256_file(path) != upload["sha256"]:
            raise ProtocolError(f"upload checksum mismatch: {name}")
    return manifest


def publish_cancel(paths: TargetPaths, job_id: str) -> None:
    try:
        write_exclusive(paths.control / "cancel" / safe_name(job_id), canonical_json({"job_id": job_id, "at_ns": now_ns()}))
    except FileExistsError:
        pass


def publish_final(result_dir: Path, result: dict[str, Any]) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    body = canonical_json(result)
    atomic_write(result_dir / "result.json", body)
    write_exclusive(result_dir / "FINAL", canonical_json({"result_sha256": sha256_bytes(body), "at_ns": now_ns()}))


def read_final(result_dir: Path) -> dict[str, Any]:
    final = read_json(result_dir / "FINAL")
    body = (result_dir / "result.json").read_bytes()
    if sha256_bytes(body) != final.get("result_sha256"):
        raise ProtocolError("result checksum mismatch")
    return read_json(result_dir / "result.json")
