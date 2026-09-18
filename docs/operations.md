# Configuration and operations

## Target registry

The client registry is TOML. Each target needs an explicit name, shared path as seen by the client, and target platform. See [`examples/config.toml`](../examples/config.toml). A watcher root is the same subtree as seen on that host; it does not read the client registry.

## Policy

Start from [`examples/policy-dev.toml`](../examples/policy-dev.toml). Empty `allowed_executables` means any executable except the denylist and is appropriate only for local development. Production should enumerate basenames, set cwd roots and low limits, disable runtimes, use a dedicated OS account, and combine [`policy-production-readonly.toml`](../examples/policy-production-readonly.toml) with real OS/application read-only permissions.

Policy changes require watcher restart. Environment names are exact and values are never logged by the watcher journal. `approval_hook` is target-local, invoked without a shell, receives the verified manifest on stdin, and runs before `STARTED`.

## Linux

Install into a dedicated virtual environment, copy [`services/fs-exec-watcher.service`](../services/fs-exec-watcher.service), edit paths/account, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fs-exec-watcher
sudo systemctl status fs-exec-watcher
```

Keep the journal on local disk, not the share. The service hardening sample is a baseline; adjust `ReadWritePaths` and executable access to the actual policy.

## Windows

Install Python 3.11+, install this package in a dedicated virtual environment, edit [`services/windows-watcher.ps1`](../services/windows-watcher.ps1), and run it under a dedicated account as a long-running PowerShell process. For boot operation, wrap that exact command with a service manager such as WinSW/NSSM, or create a Task Scheduler task “At startup”, “Run whether user is logged on or not”, restart on failure, and do not stop after a time limit. Native Windows uses `powershell.exe`; PowerShell 7 can also supply `pwsh` when appropriate.

Use a UNC path rather than a drive letter in a service (drive mappings are session-local). Grant the service account modify rights only on its target relay subtree and required cwd roots. Deny interactive login where organizational policy supports it.

## NFS versus SMB

**NFS:** use one server/export, stable UID/GID mapping, and same-filesystem request staging/rename. Attribute and directory caches can delay marker visibility. Avoid `nolock` assumptions; this protocol does not rely on advisory locks. NFSv4 is preferred. Test atomic `mkdir`, exclusive file creation, and rename from every client.

**SMB:** use UNC paths for Windows services. Oplocks/leases, directory caching, antivirus, and Offline Files can delay visibility. Disable Offline Files for the relay and exclude it from content synchronization. Do not place staging and ready paths on different shares. Test ACL inheritance and case sensitivity; fs-exec names are case-stable ASCII.

For both, event notifications may accelerate an external monitor but must never replace exact marker polling. Keep client and watcher clocks synchronized for one-way health estimates. Use the measured round-trip and conservative fallback if they are not.

## Runbook

* `health`: inspect heartbeat age/state and PING/PONG percentiles. `BUSY` is healthy.
* `AWAITING_VISIBILITY`: verify mount/path and share write propagation.
* `VISIBLE_UNCLAIMED`: verify watcher heartbeat and policy service.
* `CLAIMED` without `RUNNING`: inspect watcher logs; do not delete the claim casually.
* `AVAILABILITY_UNKNOWN`: investigate target process and side effects before any manual retry.
* Stale `watcher.lock`: stop/verify all watchers on all hosts first, then remove only the lock directory and restart one watcher.
* Cancel is best effort and idempotent; repeated cancellation preserves the first immutable marker.
* Retention: after the longest client wait and audit period, remove terminal `inbox/JOB.ready`, `claims/JOB`, `results/JOB`, cancel markers, and old probes together. Never clean non-final jobs automatically.
* Back up policy/config, not transient queue data. Alert on stale heartbeat, rejected jobs, output truncation, share capacity, and `AVAILABILITY_UNKNOWN`.

## Provenance review: cowork-to-code-bridge

On 2026-09-18 we inspected the canonical public repository [`abhinaykrupa/cowork-to-code-bridge`](https://github.com/abhinaykrupa/cowork-to-code-bridge) at revision `30b06c2fb24a916c835a2928d2d274727faab09b`. It is MIT licensed (copyright 2026 Abhi Gadikoppula). The similarly named `virtuosotravlr/cowork-bridge` is a different project.

Useful architectural ideas were atomic temporary-file rename, append-only fsync'd journal events, durable in-flight markers with no replay after crash, queue age expiry, bounded live output, cancellation of process groups, optional approval hooks, and explicit security limitations. fs-exec adapts those **concepts** independently.

The project itself does not fit as a dependency: it is a single local Mac/Linux/WSL bridge for preapproved `.sh`/`.py` scripts, uses a token and flat JSON queue/results, has no native Windows watcher, explicit target registry, request/upload checksums, COMMIT/FINAL framing, claim/fencing protocol, artifact transport, phase-specific status/timeouts, NFS/SMB consistency model, or latency percentile/auto-overhead system. Its tests strongly cover crash recovery, idempotency, cancellation trees, redaction, and output bounds, but not this relay's cross-share protocol. No source code was copied, modified, or vendored; therefore there is no derived code requiring MIT notice inclusion beyond this provenance record.
