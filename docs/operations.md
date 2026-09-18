# Configuration and operations

## Target registry

The client registry is TOML. Each target needs an explicit name, shared path as seen by the client, and target platform. See [`examples/config.toml`](../examples/config.toml). A watcher root is the same subtree as seen on that host; it does not read the client registry.

## Policy

Start from [`examples/policy-dev.toml`](../examples/policy-dev.toml). Empty executable, runtime, cwd-root and environment allowlists deny access. Enumerate existing owner-controlled **absolute executable paths**, not basenames. A client basename selects a unique configured path without searching PATH; an explicit client path must match a configured or canonical path. Keep binaries and their parent directories, interpreter modules, cwd configuration and approval hook out of client write access. Optional `[policy.executable_sha256]` maps canonical absolute paths to SHA-256 pins; it does not remove filesystem replacement races, so ownership is still required. Production should use low limits, disable runtimes, use a dedicated OS account, and combine [`policy-production-readonly.toml`](../examples/policy-production-readonly.toml) with real OS/application read-only permissions.

Policy changes require watcher restart. Environment names are case-insensitive on Windows; dangerous loader/interpreter/relay controls are denied regardless of allowlist. Service credentials and arbitrary environment are not inherited. The journal records exception types, not exception text or environment/stdin values. `approval_hook` is owner-controlled, invoked without a shell, receives the verified manifest on stdin, and runs before STARTED with cancellation/expiry checks and a 30-second budget. Its output is discarded to prevent secret leakage.

`max_upload_files` and `max_artifact_files` default to 128; `max_queue_age_seconds` defaults to 3600. This age cap bounds observed preparation and uses earlier valid client publication time when available; it is not a disk quota or proof of time spent in a dishonest client's unobserved queue. Configure server-side per-client byte/inode quotas and externally managed retention for jobs, staging directories and probes. No global queue quota, request-rate limiter or automatic retention worker is implemented.

## Directional namespace ownership is required

Provision a separate target root for each client trust domain before sharing it. Clients never provision server namespaces. The root and structural parents must not be client-writable (including delete-child, rename, ownership and ACL-change rights).

| Namespace | Client identity | Watcher identity |
| --- | --- | --- |
| `inbox`, `control/cancel`, `health/pings` | Create/publish own jobs, cancellations and probes | Read/traverse |
| `claims`, `results`, `health/pongs`, `health/heartbeat.json`, `health/watcher.lock` | Read/traverse only | Create/write/delete |
| Root, `control`, `health` parents | Traverse/read, **no delete-child or rename** | Owner-managed |

Clients within one namespace share cancellation authority. Use separate roots/watchers for mutually untrusted clients; this protocol has no per-job identity authentication. A client can mutate its own submissions; verified manifests are retained in memory and uploads reverified after bounded target-local copying. Client mutation must never grant write access to watcher-owned results or claims. If these ACLs cannot be enforced, **do not deploy**: hashes are not signatures and authenticated gateway/result signing is not implemented.

### Two-UID POSIX ACL example

For a **new, empty** root on a Linux POSIX-ACL filesystem, install `acl`, create separate `relay-watcher` and `relay-client` accounts, and run the following as the administrator. Replace paths/accounts first; do not apply recursively to an existing live queue. Give both accounts traverse access to ancestors of `$ROOT`.

```bash
ROOT=/srv/fs-exec/client-a
W=relay-watcher
C=relay-client
install -d -o "$W" -g "$W" -m 0700 "$ROOT"
for path in inbox claims results control control/cancel health health/pings health/pongs; do
  install -d -o "$W" -g "$W" -m 0700 "$ROOT/$path"
done
# Watcher-owned trees: named client is read/traverse-only, not group-writable.
for path in "$ROOT" "$ROOT"/* "$ROOT/control/cancel" "$ROOT/health/pings" "$ROOT/health/pongs"; do
  setfacl -m "u::rwx,u:$C:r-x,g::---,m::rwx,o::---" "$path"
  setfacl -d -m "u::rwx,u:$C:r-x,g::---,m::rwx,o::---" "$path"
done
# Incoming trees: client creates entries; new client-owned entries grant watcher RX.
for path in inbox control/cancel health/pings; do
  setfacl -m "u:$C:rwx" "$ROOT/$path"
  setfacl -k "$ROOT/$path"
  setfacl -d -m "u::rwx,u:$W:r-x,g::---,m::rwx,o::---" "$ROOT/$path"
done
```

Shared transport file creation uses mode 0660 and shared directory creation 0770 **as ACL masks**, preserving administrator-specified named reader grants; it does not chmod existing files or grant the client write access to server trees. With a default ACL, POSIX inheritance takes precedence over umask; without one, `UMask=0077` keeps new files private and a second UID cannot use the relay. Keep that service umask for local secrets. Private materialized uploads, stdin and downloaded artifact temporary files use 0600. Do not “fix” access with recursive `chmod 777` or a shared writable group.

The regression `DirectionalACLTests` actually submits as `nobody`, executes as the invoking UID, reads verified output as `nobody`, and checks denied writes to FINAL, CLAIM and watcher.lock under `umask 0077`. It runs when Linux libacl and passwordless `sudo -u nobody` are available; otherwise it is explicitly skipped. This validates local POSIX ACLs, not NAS ACL translation. Verify submit, health, cancel, output, artifacts, denied write/delete/rename and directory durability on your actual accounts/mounts before deployment.

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

Use a UNC path rather than a drive letter in a service (drive mappings are session-local). Apply the directional ACL table above, not client Modify on the whole subtree. Deny interactive login where organizational policy supports it. Native Windows remains unverified here: no Windows Job Object, no portable directory flush, best-effort reparse-point checks, and no proof of arbitrary program command-line parsing. Do not claim production containment from the command-shape unit tests.

## NFS versus SMB

**NFS:** use one server/export, stable UID/GID mapping, and same-filesystem staging/rename/hard-links. Attribute and directory caches can delay visibility. NFSv4 ACL inheritance must preserve named client read/traverse grants on server trees while denying WRITE_DATA, ADD_FILE, ADD_SUBDIRECTORY, DELETE, DELETE_CHILD, WRITE_ACL and WRITE_OWNER there and on protected parents. Grant create/write only in the incoming trees. POSIX default ACL syntax is not an NFSv4 ACL recipe: inspect the server's effective ACLs from both UIDs. Test atomic mkdir, no-replace hard-link publication, rename and fsync persistence from every client.

**SMB:** use UNC paths for Windows services. Configure both share and NTFS ACLs: client Read/List/Traverse on server trees, with no Write/Modify/Delete/Delete child/Change permissions/Take ownership; watcher owns those trees. Limit client Create files/Create folders/Write data/rename to incoming trees and their own entries. Remove broad inherited Modify grants on the root. Oplocks/leases, antivirus and Offline Files can delay visibility; disable Offline Files and content synchronization. Test hard-link support, effective inherited ACLs, reparse rejection, case handling and crash persistence. Linux POSIX ACL tests do not verify SMB mapping.

For both, event notifications may accelerate an external monitor but must never replace exact marker polling. Keep client and watcher clocks synchronized for one-way health estimates. Use the measured round-trip and conservative fallback if they are not.

## Runbook

* `health`: inspect heartbeat age/state and PING/PONG percentiles. `BUSY` is healthy. Exit is nonzero for stale/stopped/unavailable watchers or an unsuccessful requested probe; `--no-probe` checks heartbeat only.
* `AWAITING_VISIBILITY`: verify mount/path and share write propagation.
* `VISIBLE_UNCLAIMED`: verify watcher heartbeat and policy service.
* `CLAIMED` without `RUNNING`: inspect watcher logs; do not delete the claim casually.
* `AVAILABILITY_UNKNOWN`: investigate target process and side effects before any manual retry.
* Stale `watcher.lock`: stop/verify all watchers on all hosts first, then remove only the lock directory and restart one watcher.
* Cancel is best effort and idempotent; repeated cancellation preserves the first immutable marker.
* Retention: after the longest client wait and audit period, the administrator may remove terminal inbox/results/cancel data and old probes. Retain claims as tombstones while the namespace accepts work; deleting claims permits job-ID reuse/replay. Retire and stop an entire namespace before deleting its tombstones. Never clean ambiguous jobs automatically.
* Back up policy/config, not transient queue data. Alert on stale heartbeat, rejected jobs, output truncation, share capacity, and `AVAILABILITY_UNKNOWN`.

## Provenance review: cowork-to-code-bridge

On 2026-09-18 we inspected the canonical public repository [`abhinaykrupa/cowork-to-code-bridge`](https://github.com/abhinaykrupa/cowork-to-code-bridge) at revision `30b06c2fb24a916c835a2928d2d274727faab09b`. It is MIT licensed (copyright 2026 Abhi Gadikoppula). The similarly named `virtuosotravlr/cowork-bridge` is a different project.

Useful architectural ideas were atomic temporary-file rename, append-only fsync'd journal events, durable in-flight markers with no replay after crash, queue age expiry, bounded live output, cancellation of process groups, optional approval hooks, and explicit security limitations. fs-exec adapts those **concepts** independently.

The project itself does not fit as a dependency: it is a single local Mac/Linux/WSL bridge for preapproved `.sh`/`.py` scripts, uses a token and flat JSON queue/results, has no native Windows watcher, explicit target registry, request/upload checksums, COMMIT/FINAL framing, claim/fencing protocol, artifact transport, phase-specific status/timeouts, NFS/SMB consistency model, or latency percentile/auto-overhead system. Its tests cover crash recovery, idempotency, cancellation trees, redaction, and output bounds, but not this relay's cross-share protocol. No source-level copying was identified by the independent repository review; the design concepts were independently reimplemented. On the evidence reviewed, no additional upstream notice appeared required. Repository inspection alone is not proof of authorship or a definitive legal determination; preserve upstream MIT notices if source is incorporated later.
