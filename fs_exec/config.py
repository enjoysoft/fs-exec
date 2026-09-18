from __future__ import annotations

import math
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .util import safe_name


@dataclass(frozen=True)
class Target:
    name: str
    path: Path
    platform: str = "linux"
    description: str = ""
    transport_timeout: float | None = None


@dataclass(frozen=True)
class Policy:
    allowed_executables: frozenset[str] = frozenset()
    allowed_runtimes: frozenset[str] = frozenset()
    executable_sha256: dict[str, str] = field(default_factory=dict)
    cwd_roots: tuple[Path, ...] = ()
    environment_allowlist: frozenset[str] = frozenset()
    max_runtime_seconds: float = 3600
    max_request_bytes: int = 1024 * 1024
    max_output_bytes: int = 16 * 1024 * 1024
    max_artifact_bytes: int = 128 * 1024 * 1024
    max_upload_bytes: int = 128 * 1024 * 1024
    max_upload_files: int = 128
    max_artifact_files: int = 128
    max_queue_age_seconds: float = 3600
    read_only: bool = False
    approval_hook: Path | None = None
    deny_executables: frozenset[str] = frozenset({"sudo", "su", "doas", "fs-exec", "fs-exec-watcher"})
    deny_environment_fragments: tuple[str, ...] = ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PRIVATE_KEY")

    def __post_init__(self) -> None:
        for name in ("max_runtime_seconds", "max_queue_age_seconds"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be finite and positive")
        for name in ("max_request_bytes", "max_output_bytes", "max_artifact_bytes", "max_upload_bytes", "max_upload_files", "max_artifact_files"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")


@dataclass(frozen=True)
class Registry:
    targets: dict[str, Target] = field(default_factory=dict)

    def target(self, name: str) -> Target:
        try:
            return self.targets[name]
        except KeyError as exc:
            raise ValueError(f"unknown target {name!r}; use 'fs-exec targets'") from exc


def default_config_path() -> Path:
    configured = os.environ.get("FS_EXEC_CONFIG")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".config" / "fs-exec" / "config.toml"


def load_registry(path: Path | None = None) -> Registry:
    path = path or default_config_path()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    targets: dict[str, Target] = {}
    for name, value in raw.get("targets", {}).items():
        safe_name(name)
        if not isinstance(value, dict) or "path" not in value:
            raise ValueError(f"target {name!r} requires path")
        platform = str(value.get("platform", "linux")).lower()
        if platform not in {"linux", "windows"}:
            raise ValueError(f"target {name!r} platform must be linux or windows")
        targets[name] = Target(
            name=name,
            path=Path(str(value["path"])).expanduser(),
            platform=platform,
            description=str(value.get("description", "")),
            transport_timeout=float(value["transport_timeout"]) if "transport_timeout" in value else None,
        )
    return Registry(targets)


def load_policy(path: Path) -> Policy:
    with path.open("rb") as handle:
        raw: dict[str, Any] = tomllib.load(handle).get("policy", {})
    return Policy(
        allowed_executables=frozenset(map(str, raw.get("allowed_executables", []))),
        allowed_runtimes=frozenset(map(str, raw.get("allowed_runtimes", []))),
        executable_sha256=dict(raw.get("executable_sha256", {})),
        cwd_roots=tuple(Path(str(p)).expanduser().resolve() for p in raw.get("cwd_roots", [])),
        environment_allowlist=frozenset(map(str, raw.get("environment_allowlist", []))),
        max_runtime_seconds=float(raw.get("max_runtime_seconds", 3600)),
        max_request_bytes=int(raw.get("max_request_bytes", 1024 * 1024)),
        max_output_bytes=int(raw.get("max_output_bytes", 16 * 1024 * 1024)),
        max_artifact_bytes=int(raw.get("max_artifact_bytes", 128 * 1024 * 1024)),
        max_upload_bytes=int(raw.get("max_upload_bytes", 128 * 1024 * 1024)),
        max_upload_files=int(raw.get("max_upload_files", 128)),
        max_artifact_files=int(raw.get("max_artifact_files", 128)),
        max_queue_age_seconds=float(raw.get("max_queue_age_seconds", 3600)),
        read_only=bool(raw.get("read_only", False)),
        approval_hook=Path(str(raw["approval_hook"])).expanduser().resolve() if raw.get("approval_hook") else None,
        deny_executables=frozenset(map(str, raw.get("deny_executables", ["sudo", "su", "doas", "fs-exec", "fs-exec-watcher"]))),
        deny_environment_fragments=tuple(map(str, raw.get("deny_environment_fragments", ["TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PRIVATE_KEY"]))),
    )
