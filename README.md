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

1. Provision and mount one shared directory on client and target. Apply the [directional ACL recipe](docs/operations.md#directional-namespace-ownership-is-required): clients must not write results, claims, heartbeat or lock state. The owner creates the namespace first; clients no longer create it.
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

## When the sandbox cannot mount the real share

Install the included `fs-rally` sidecar outside the sandbox. The client writes to
an allowed drop directory; the sidecar forwards only protocol-v2 requests/cancel/
pings outward and verified results/status/health inward. It never executes commands.
The watcher remains on the real share under its existing policy and separate account.

Start with [`examples/rally.toml`](examples/rally.toml) and the
[relay deployment guide](docs/rally.md), including mandatory directional ACLs,
fresh-namespace migration, limits, service samples and native-platform test gates.
Use `fs-rally --config rally.toml dry-run` before `run`. Relayed output waits for
FINAL integrity verification; status and cancellation still work while jobs run.

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

This is a single-watcher-per-target relay, not a distributed scheduler. Every pre-existing crash-recovery claim is reported `AVAILABILITY_UNKNOWN` and is never replayed automatically, even when STARTED is not visible. Duplicate suppression depends on retained claims and tested server atomicity/durability; it does not promise exactly-once effects or safe live failover. Protocol v2 requires atomic hard-links and coordinated client/watcher upgrades. Executable/runtime/cwd/environment allowlists default to deny-all; configure absolute owner-controlled executable paths. Live output is provisional until FINAL verification. Shell mode cannot infer read-only behavior, and filesystem ACLs—not hashes—authenticate the writer. Linux/local tests do not establish native Windows or NFS/SMB correctness. See the deployment gates and limitations in the docs.

## Development

```bash
python -m unittest discover -s tests -v
python -m compileall -q fs_exec tests
```

## License

MIT. See [LICENSE](LICENSE).
