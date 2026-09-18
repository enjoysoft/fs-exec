from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .config import Policy
from .util import check_path, safe_name, sha256_file


class PolicyError(RuntimeError):
    pass


def executable_for(value: str, policy: Policy) -> str:
    # Only resolve owner-supplied paths, never a client basename through PATH.
    candidates = []
    for configured in policy.allowed_executables:
        path = Path(configured)
        if not path.is_absolute():
            raise PolicyError("allowlisted executables must be absolute paths")
        canonical = path.resolve(strict=True)
        if value == configured or value == str(canonical) or value == path.name:
            candidates.append(canonical)
    if len(set(candidates)) != 1:
        raise PolicyError("executable is not uniquely allowlisted")
    executable = candidates[0]
    if executable.name.lower() in {item.lower() for item in policy.deny_executables}:
        raise PolicyError("executable is denied")
    if executable.suffix.lower() in {".bat", ".cmd"}:
        raise PolicyError("Windows batch executables are not supported")
    if not executable.is_file():
        raise PolicyError("executable is not a regular file")
    expected = policy.executable_sha256.get(str(executable))
    if expected and sha256_file(executable) != expected:
        raise PolicyError("executable checksum mismatch")
    return str(executable)


def command_for(manifest: dict[str, Any], platform: str, policy: Policy) -> list[str]:
    mode = manifest.get("mode", "argv")
    if mode == "argv":
        argv = manifest.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
            raise PolicyError("argv mode requires a non-empty string array")
        return [executable_for(argv[0], policy), *argv[1:]]
    if mode != "shell":
        raise PolicyError(f"unsupported mode: {mode}")
    runtime = str(manifest.get("runtime") or ("powershell" if platform == "windows" else "bash")).lower()
    if runtime not in policy.allowed_runtimes:
        raise PolicyError(f"runtime is not allowlisted: {runtime}")
    script = manifest.get("script")
    if not isinstance(script, str):
        raise PolicyError("shell mode requires script text")
    if runtime == "bash":
        return [executable_for("bash", policy), "--noprofile", "--norc", "-euo", "pipefail", "-c", script]
    if runtime == "powershell":
        executable = "powershell.exe" if platform == "windows" else "pwsh"
        return [executable_for(executable, policy), "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script]
    raise PolicyError(f"unsupported runtime: {runtime}")


def checked_cwd(value: str | None, policy: Policy) -> Path:
    check_path(Path(value or os.getcwd()).expanduser())
    cwd = Path(value or os.getcwd()).expanduser().resolve()
    if not any(cwd == root or root in cwd.parents for root in policy.cwd_roots):
        raise PolicyError(f"cwd is outside allowed roots: {cwd}")
    if not cwd.is_dir():
        raise PolicyError(f"cwd does not exist: {cwd}")
    return cwd


def checked_environment(values: dict[str, Any], policy: Policy) -> dict[str, str]:
    # Owner's service environment must itself be trusted; no inherited PATH,
    # HOME or language-specific startup settings. Program paths are absolute.
    environment = {"PATH": os.defpath, "LANG": "C.UTF-8"}
    if os.name == "nt":
        environment = {key: value for key, value in os.environ.items() if key.upper() in {"SYSTEMROOT", "WINDIR", "TEMP", "TMP"}}
        environment["PATH"] = str(Path(os.environ["SystemRoot"]) / "System32")
    forbidden = {"PATH", "HOME", "USERPROFILE", "COMSPEC", "PATHEXT", "SYSTEMROOT", "WINDIR", "BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS", "IFS", "CDPATH", "GCONV_PATH", "LOCPATH", "NLSPATH", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "RUBYOPT", "RUBYLIB", "PERL5OPT", "PERL5LIB", "PSMODULEPATH", "DOTNET_STARTUP_HOOKS", "TMP", "TEMP", "TMPDIR"}
    seen: set[str] = set()
    for key, value in values.items():
        safe_name(key)
        upper = key.upper()
        if upper in seen or upper in forbidden or upper.startswith(("LD_", "DYLD_", "PYTHON", "NODE_", "FS_EXEC_")):
            raise PolicyError("unsafe environment variable")
        seen.add(upper)
        allowed = {item.upper() if os.name == "nt" else item for item in policy.environment_allowlist}
        if (upper if os.name == "nt" else key) not in allowed:
            raise PolicyError("environment variable is not allowlisted")
        if any(fragment.upper() in upper for fragment in policy.deny_environment_fragments):
            raise PolicyError("credential-like environment variable is denied")
        environment[key] = str(value)
    environment["FS_EXEC_RELAY"] = "1"
    if policy.read_only:
        environment["FS_EXEC_READ_ONLY"] = "1"
    return environment
