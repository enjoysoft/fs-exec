from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

from .client import Client, JobStatus
from .config import default_config_path, load_policy, load_registry
from .watcher import Watcher


def _json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _env(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"environment value must be NAME=VALUE: {value!r}")
        key, item = value.split("=", 1)
        result[key] = item
    return result


def _client(args: argparse.Namespace) -> Client:
    registry = load_registry(Path(args.config))
    return Client(registry.target(args.target))


def _transport(client: Client, value: str) -> float:
    if value != "auto":
        return float(value)
    configured = client.target.transport_timeout
    return configured if configured is not None else client.latency.stats().recommended_overhead


def _status_exit(status: JobStatus) -> int:
    if not status.result:
        return 124
    if status.result.get("status") == "COMPLETED":
        code = int(status.result.get("exit_code") or 0)
        return 128 + abs(code) if code < 0 else min(code, 255)
    if status.result.get("status") in {"TIMED_OUT", "CANCELLED"}:
        return 124 if status.result["status"] == "TIMED_OUT" else 130
    return 125


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fs-exec", description="Filesystem-backed authorized remote execution relay")
    parser.add_argument("--config", default=str(default_config_path()), help="target registry TOML")
    sub = parser.add_subparsers(dest="subcommand", required=True)

    targets = sub.add_parser("targets", help="list configured targets")
    targets.set_defaults(handler=cmd_targets)

    for name in ("status", "wait", "cancel"):
        command = sub.add_parser(name)
        command.add_argument("--target", required=True)
        command.add_argument("job_id")
        if name == "wait":
            command.add_argument("--total-timeout", type=float, default=3600)
            command.add_argument("--stream", action=argparse.BooleanOptionalAction, default=True)
            command.add_argument("--output-dir", type=Path)
        command.set_defaults(handler={"status": cmd_status, "wait": cmd_wait, "cancel": cmd_cancel}[name])

    health = sub.add_parser("health")
    health.add_argument("--target", required=True)
    health.add_argument("--transport-timeout", default="auto")
    health.add_argument("--no-probe", action="store_true")
    health.set_defaults(handler=cmd_health)

    execute = sub.add_parser("exec", help="submit and wait (default when options precede --)")
    execute.add_argument("--target", required=True)
    execute.add_argument("--cwd")
    execute.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    execute.add_argument("--stdin", dest="stdin_text")
    execute.add_argument("--input-file", type=Path)
    execute.add_argument("--upload", action="append", type=Path, default=[])
    execute.add_argument("--artifact", action="append", default=[], help="remote cwd glob to return")
    execute.add_argument("--output-dir", type=Path)
    execute.add_argument("--timeout", "--command-timeout", dest="command_timeout", type=float, default=60)
    execute.add_argument("--total-timeout", "--tool-call-timeout", dest="total_timeout", type=float)
    execute.add_argument("--transport-timeout", default="auto", help="seconds or auto")
    execute.add_argument("--queue-timeout", type=float)
    execute.add_argument("--result-timeout", type=float, help="maximum result propagation wait after result.json appears")
    execute.add_argument("--expiry", type=float, default=3600)
    execute.add_argument("--detach", action="store_true")
    execute.add_argument("--script", help="explicit shell script (never inferred)")
    execute.add_argument("--runtime", choices=["bash", "powershell"])
    execute.add_argument("command", nargs=argparse.REMAINDER)
    execute.set_defaults(handler=cmd_exec)
    return parser


def cmd_targets(args: argparse.Namespace) -> int:
    registry = load_registry(Path(args.config))
    _json({name: {"path": str(target.path), "platform": target.platform, "description": target.description} for name, target in registry.targets.items()})
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    _json(asdict(_client(args).status(args.job_id)))
    return 0


def cmd_wait(args: argparse.Namespace) -> int:
    client = _client(args)
    status = client.wait(args.job_id, args.total_timeout, stream=args.stream)
    if status.result and args.output_dir:
        client.download_artifacts(args.job_id, args.output_dir)
    if not args.stream or not status.result:
        _json(asdict(status))
    if not status.result:
        print(f"fs-exec: client timeout; job may still run; job_id={args.job_id}", file=sys.stderr)
    return _status_exit(status)


def cmd_cancel(args: argparse.Namespace) -> int:
    _client(args).cancel(args.job_id)
    print(args.job_id)
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    client = _client(args)
    _json(client.health(probe=not args.no_probe, timeout=_transport(client, args.transport_timeout)))
    return 0


def cmd_exec(args: argparse.Namespace) -> int:
    client = _client(args)
    started = time.monotonic()
    command = list(args.command)
    if command[:1] == ["--"]:
        command = command[1:]
    if args.script is None and not command:
        raise ValueError("command required after --, or use --script")
    if args.script is not None and command:
        raise ValueError("--script and argument-array command are mutually exclusive")
    job_id = client.submit(
        argv=command,
        script=args.script,
        runtime=args.runtime,
        cwd=args.cwd,
        env=_env(args.env),
        stdin=args.stdin_text,
        stdin_file=args.input_file,
        uploads=args.upload,
        artifacts=args.artifact,
        command_timeout=args.command_timeout,
        expiry=args.expiry,
    )
    print(f"fs-exec: submitted job_id={job_id}", file=sys.stderr)
    if args.detach:
        print(job_id)
        return 0
    transport = _transport(client, args.transport_timeout)
    total = args.total_timeout if args.total_timeout is not None else args.command_timeout + transport
    remaining = lambda: max(0.0, total - (time.monotonic() - started))
    if not client.wait_for_phase(job_id, "visibility", min(transport, remaining())):
        print(f"fs-exec: request visibility timeout; outcome ambiguous; job_id={job_id}", file=sys.stderr)
        return 124
    queue_timeout = min(args.queue_timeout if args.queue_timeout is not None else transport, remaining())
    if not client.wait_for_phase(job_id, "claim", queue_timeout):
        print(f"fs-exec: queue/claim timeout; job was not resubmitted; job_id={job_id}", file=sys.stderr)
        return 124
    status = client.wait(job_id, remaining(), stream=True, result_timeout=args.result_timeout or transport)
    if status.result and args.output_dir:
        client.download_artifacts(job_id, args.output_dir)
    if not status.result:
        print(f"fs-exec: total timeout; job may still run; job_id={job_id}", file=sys.stderr)
    elif status.result.get("error"):
        print(f"fs-exec: {status.result['status']}: {status.result['error']}", file=sys.stderr)
    return _status_exit(status)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    values = list(argv if argv is not None else sys.argv[1:])
    known = {"exec", "status", "wait", "cancel", "health", "targets"}
    # Shell-like convenience: options without a named subcommand mean exec.
    command_end = values.index("--") if "--" in values else len(values)
    if not any(value in known for value in values[:command_end]) and values and values[0] not in {"-h", "--help"}:
        insertion = 2 if values[:1] == ["--config"] and len(values) >= 2 else 1 if values[0].startswith("--config=") else 0
        values.insert(insertion, "exec")
    try:
        args = parser.parse_args(values)
        return int(args.handler(args))
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"fs-exec: {exc}", file=sys.stderr)
        return 125


def watcher_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fs-exec-watcher")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--platform", choices=["linux", "windows"])
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    watcher = Watcher(args.root, load_policy(args.policy), platform=args.platform, poll_interval=args.poll_interval, journal=args.journal)
    try:
        watcher.run(once=args.once)
    except KeyboardInterrupt:
        watcher.stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
