# fs-exec

`fs-exec` is a filesystem-backed, shell-like **authorized remote execution relay**. A client publishes a checksummed job into an explicitly selected target inbox; one watcher on that target validates policy, executes it, and publishes streamed output, artifacts, and a final result. It opens no network listener and uses only Python 3.11+ standard-library modules.

```bash
python -m pip install .
fs-exec --target prod-linux --timeout 60 -- curl -sS https://example.com/
```

Argument-array execution is the default and always uses `shell=False`. Shell parsing is opt-in:

```bash
fs-exec --target dev-linux --script 'set -x; make test | tee test.log' --runtime bash
fs-exec --target windows --script 'Get-ComputerInfo | ConvertTo-Json' --runtime powershell
```

## Quick start

1. Mount one shared directory on client and target. Restrict it with OS/filesystem ACLs.
2. Copy and edit [`examples/config.toml`](examples/config.toml) on the client and [`examples/policy-dev.toml`](examples/policy-dev.toml) on the target.
3. Start the target watcher:

   ```bash
   fs-exec-watcher --root /srv/fs-exec/dev-linux --policy /etc/fs-exec/policy.toml \
     --journal /var/lib/fs-exec/journal.jsonl
   ```

4. Probe it and execute:

   ```bash
   fs-exec health --target dev-linux
   fs-exec --target dev-linux --timeout 30 -- python3 -c 'print("hello")'
   ```

Set `FS_EXEC_CONFIG=/path/config.toml` or pass global `--config` before the subcommand. `fs-exec exec ...` is equivalent to the shell-like form above.

## CLI

```text
fs-exec targets
fs-exec health --target TARGET [--transport-timeout auto|SECONDS]
fs-exec --target TARGET [execution options] -- PROGRAM ARG...
fs-exec status --target TARGET JOB_ID
fs-exec wait --target TARGET [--total-timeout SECONDS] JOB_ID
fs-exec cancel --target TARGET JOB_ID
```

Execution supports `--cwd`, repeatable `--env NAME=VALUE`, `--stdin`, `--input-file`, repeatable `--upload`, repeatable return `--artifact GLOB`, `--output-dir`, command `--timeout`, `--total-timeout`/`--tool-call-timeout`, `--transport-timeout`, `--queue-timeout`, `--result-timeout`, `--expiry`, and `--detach`. Uploaded files are checksummed and exposed read-only-by-convention at `$FS_EXEC_UPLOAD_DIR`; use `--input-file` to pipe one to stdin.

A client-side timeout exits 124, prints the job ID, does **not** mark the job failed, and never resubmits. Inspect it with `status` or `wait`. A completed remote command returns its exit code; relay rejection returns 125 and cancellation 130.

## Examples

```bash
# curl, with the URL kept as one literal argv item
fs-exec --target prod-linux --timeout 30 -- curl -fsS https://internal.example/health

# PostgreSQL client; provide credentials through a target-local mechanism, not --env
fs-exec --target prod-linux --cwd /srv/app -- psql service=reporting -c 'select now()'

# Input, upload, and returned artifact
fs-exec --target dev-linux --input-file query.sql --artifact result.csv --output-dir ./out \
  -- bash -c 'psql service=dev < "$FS_EXEC_UPLOAD_DIR/query.sql" > result.csv'

# Explicit Bash and PowerShell modes
fs-exec --target dev-linux --script 'git status --short && make test' --runtime bash
fs-exec --target windows --script 'Get-ChildItem Env:TEMP' --runtime powershell
```

See [architecture and protocol](docs/architecture.md), [security and threat model](docs/security.md), and [operations/setup](docs/operations.md).

## Status and limitations

This MVP is deliberately an at-most-once, single-watcher-per-target relay. It is not a distributed scheduler. A crash after `STARTED` is reported `AVAILABILITY_UNKNOWN` and is never replayed automatically. Shell mode cannot safely infer whether a script is read-only. Filesystem ACLs are the authentication boundary; hashes detect corruption, not a malicious writer with share access. See the full limitations in the docs.

## Development

```bash
python -m unittest discover -s tests -v
python -m compileall -q fs_exec tests
```

## License

MIT. See [LICENSE](LICENSE).
