from __future__ import annotations

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
    allowed_runtimes: frozenset[str] = frozenset({"bash", "powershell"})
    cwd_roots: tuple[Path, ...] = ()
    environment_allowlist: frozenset[str] = frozenset()
    max_runtime_seconds: float = 3600
    max_request_bytes: int = 1024 * 1024
    max_output_bytes: int = 16 * 1024 * 1024
    max_artifact_bytes: int = 128 * 1024 * 1024
    max_upload_bytes: int = 128 * 1024 * 1024
    read_only: bool = False
    approval_hook: Path | None = None
    deny_executables: frozenset[str] = frozenset({"sudo", "su", "doas", "fs-exec", "fs-exec-watcher"})
    deny_environment_fragments: tuple[str, ...] = ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PRIVATE_KEY")


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
        allowed_runtimes=frozenset(map(str, raw.get("allowed_runtimes", ["bash", "powershell"]))),
        cwd_roots=tuple(Path(str(p)).expanduser().resolve() for p in raw.get("cwd_roots", [])),
        environment_allowlist=frozenset(map(str, raw.get("environment_allowlist", []))),
        max_runtime_seconds=float(raw.get("max_runtime_seconds", 3600)),
        max_request_bytes=int(raw.get("max_request_bytes", 1024 * 1024)),
        max_output_bytes=int(raw.get("max_output_bytes", 16 * 1024 * 1024)),
        max_artifact_bytes=int(raw.get("max_artifact_bytes", 128 * 1024 * 1024)),
        max_upload_bytes=int(raw.get("max_upload_bytes", 128 * 1024 * 1024)),
        read_only=bool(raw.get("read_only", False)),
        approval_hook=Path(str(raw["approval_hook"])).expanduser().resolve() if raw.get("approval_hook") else None,
        deny_executables=frozenset(map(str, raw.get("deny_executables", ["sudo", "su", "doas", "fs-exec", "fs-exec-watcher"]))),
        deny_environment_fragments=tuple(map(str, raw.get("deny_environment_fragments", ["TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PRIVATE_KEY"]))),
    )
