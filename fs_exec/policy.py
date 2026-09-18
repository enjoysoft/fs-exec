from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .config import Policy
from .util import safe_name


class PolicyError(RuntimeError):
    pass


def command_for(manifest: dict[str, Any], platform: str, policy: Policy) -> list[str]:
    mode = manifest.get("mode", "argv")
    if mode == "argv":
        argv = manifest.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
            raise PolicyError("argv mode requires a non-empty string array")
        executable = Path(argv[0]).name.lower()
        if executable in {item.lower() for item in policy.deny_executables}:
            raise PolicyError(f"executable is denied: {executable}")
        if policy.allowed_executables and executable not in {item.lower() for item in policy.allowed_executables}:
            raise PolicyError(f"executable is not allowlisted: {executable}")
        return list(argv)
    if mode != "shell":
        raise PolicyError(f"unsupported mode: {mode}")
    runtime = str(manifest.get("runtime") or ("powershell" if platform == "windows" else "bash")).lower()
    if runtime not in policy.allowed_runtimes:
        raise PolicyError(f"runtime is not allowlisted: {runtime}")
    script = manifest.get("script")
    if not isinstance(script, str):
        raise PolicyError("shell mode requires script text")
    if runtime == "bash":
        return ["bash", "-euo", "pipefail", "-c", script]
    if runtime == "powershell":
        executable = "powershell.exe" if platform == "windows" else "pwsh"
        return [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script]
    raise PolicyError(f"unsupported runtime: {runtime}")


def checked_cwd(value: str | None, policy: Policy) -> Path:
    cwd = Path(value or os.getcwd()).expanduser().resolve()
    if policy.cwd_roots and not any(cwd == root or root in cwd.parents for root in policy.cwd_roots):
        raise PolicyError(f"cwd is outside allowed roots: {cwd}")
    if not cwd.is_dir():
        raise PolicyError(f"cwd does not exist: {cwd}")
    return cwd


def checked_environment(values: dict[str, Any], policy: Policy) -> dict[str, str]:
    # Do not accidentally export the service account's credentials. Keep only
    # process-launch essentials; callers must request all other allowed values.
    essentials = {"PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TMP", "TEMP", "TMPDIR", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT"}
    environment = {
        key: value for key, value in os.environ.items()
        if key.upper() in essentials and not any(fragment in key.upper() for fragment in policy.deny_environment_fragments)
    }
    for key, value in values.items():
        safe_name(key)
        if policy.environment_allowlist and key not in policy.environment_allowlist:
            raise PolicyError(f"environment variable is not allowlisted: {key}")
        upper = key.upper()
        if any(fragment in upper for fragment in policy.deny_environment_fragments):
            raise PolicyError(f"credential-like environment variable is denied: {key}")
        environment[key] = str(value)
    environment["FS_EXEC_RELAY"] = "1"
    if policy.read_only:
        environment["FS_EXEC_READ_ONLY"] = "1"
    return environment
