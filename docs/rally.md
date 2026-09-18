# fs-rally: a directional protocol-v2 sidecar

`fs-rally` is installed by `pip install .` with fs-exec. It **never executes
commands**, invokes a shell, imports job code, or runs external sync tools. Run
it outside the sandbox, where it can access both the allowed drop and the real
NFSv4/SMB share. The sandbox client uses the drop target; the existing watcher
uses the outside target and retains all execution policy and claim ownership.

This requires an authorized file/process channel spanning the environments. If
no directory is visible to both the sandbox and an outside process, no sidecar
can bridge the gap. This is not a sandbox escape or permission bypass.

## Configuration and commands

Copy [examples/rally.toml](../examples/rally.toml). All roots are absolute,
distinct, non-overlapping, non-symlink paths **as seen by the sidecar**. Each
target maps one single-component drop name to one outside name. Multiple
clients may share a target only if they are mutually trusted as protocol writers.
Use separate target mappings and ACLs for separate trust domains. Do not mix
direct writers with a relay on the same outside target.

The only backend is the portable standard-library Python backend; Windows adds
stdlib ctypes handle checks. Unknown backends fail, rather than falling back to
an unsafe recursive copy. `direction = "protocol-v2"` declares the fixed routing
below, not unrestricted bidirectional synchronization. No path or command from
a request controls transport configuration.

```text
fs-rally --config /etc/fs-exec/rally.toml dry-run
fs-rally --config /etc/fs-exec/rally.toml once
fs-rally --config /etc/fs-exec/rally.toml run
fs-rally --config /etc/fs-exec/rally.toml status
fs-rally --config /etc/fs-exec/rally.toml health
```

The administrator provisions both targets using `TargetPaths(root).initialize()`
before assigning ACLs. Rally refuses missing structural namespaces and does not
repair ACLs. The state directory is local durable storage, inaccessible to both
client and watcher accounts. Do not put SQLite on NFS/SMB or in either root.
Only one relay process may use the local state; an OS lock releases on process
exit. Also run **only one relay for a mapping**, including across hosts and
different state directories. There is no distributed relay election.

`dry-run` validates currently visible source publications with an in-memory
journal and writes nothing, including no heartbeat or state directory. It does
not certify destination permissions or compare against an existing live journal.
`once` performs one bounded sweep; a successful sweep is not proof that all
pending filesystem data has become visible. `run` retries continuously with
jittered exponential polling backoff, resetting after transfers. `status` and
`health` read the last local health snapshot without taking the writer lock;
they do not initiate client probes. Stdout is JSON; stderr contains structured
JSON events with operation/error classes, not request contents. Exit codes:
0 = successful sweep/fresh healthy status, 1 = deferred/degraded/pending/stale,
2 = configuration/state/startup failure, 130 = interrupted. In `run`, transient
publication failures degrade health and retry rather than terminating the daemon.

## Exact allowlist and publication rules

Paths below are relative to each mapped target. Components use v2's ASCII safe
names, additionally rejecting `.`/`..`, Windows device names, ADS, backslashes,
absolute paths, empty components, and excessive length/depth. Case-aliasing
target names, upload names, and artifact names are rejected.

| Direction | Accepted publications |
|---|---|
| Drop → shared | `inbox/J.ready/request.json`, manifest-listed `uploads/NAME`, and `COMMIT` last |
| Drop → shared | `control/cancel/J` complete immutable JSON |
| Drop → shared | `health/pings/P.ready/PING` complete immutable JSON |
| Shared → drop | `results/J/CLAIMED`, `results/J/RUNNING` complete immutable identity records |
| Shared → drop | `results/J/result-HEX32.json` selected by FINAL; manifest-bound `stdout/NNNNNNNN.chunk`, `stderr/NNNNNNNN.chunk`, `artifacts/RELATIVE`; `FINAL` last |
| Shared → drop | `health/pongs/P`, matched to a known client ping; `health/heartbeat.json` (mutable, non-regressing timestamp) |
| Relay-owned local output | Drop `health/rally.json` and private state `health.json` |

There is **no claims-tree synchronization**, because clients use CLAIMED/RUNNING
under results. No watcher lock, claim, STARTED tombstone, policy, unrelated root
file, or unreferenced artifact is copied or deleted. Reverse-direction content
is not used as a source: a fake drop FINAL cannot modify the shared FINAL.
Incoming `.staging` and `.tmp` names are unpublished and never forwarded. Unknown
request/ping tree entries reject that publication. Result trees may contain
older well-named result generations and provisional numeric chunks after watcher
recovery; those are not forwarded unless selected by FINAL. Other unknown result
paths reject that publication. Root areas outside the allowlist are ignored,
never recursively mirrored.

The relay reads a bounded stable snapshot, validates complete JSON (no duplicate
keys, concatenated objects or NaN), manifest identities, sizes, SHA-256 hashes and
chunk counts, then rereads the snapshot for mutation. It rejects symlinks/reparse
points, special files, unexpected hard links, sparse files and observed growth
or replacement. POSIX opens pin ancestors with no-follow descriptors. Windows
source reads open ancestors without delete sharing and the leaf without write
sharing, inspecting handle attributes without following reparse points. Shared
destination directories and all structural roots still require trusted ACLs.

Validated bytes and their direction/target/path/content identity are committed
to a synchronous SQLite transaction **before** any destination writes. Files
are fsynced in destination-local temporary names and atomically hard-linked
without replacement, then reread; payloads and generations precede COMMIT/FINAL.
Shares must support these v2 atomic hard links; unsupported filesystems fail
closed. Recovery can clean only a journal-bound temporary name. A leftover
publication hard link is accepted only when its two names identify the same
inode, link count is exactly two, and content equals the journal bytes.

The first valid snapshot recorded for an identity wins, even after a crash or
source mutation. A conflicting source or existing destination is deferred with
diagnostics, never overwritten or automatically deleted. Pending journal bytes
replay before discovery on restart. Completed identities persist after payload
blobs are discarded; a vanished completed destination is **not resubmitted**.
Keep the journal, claims and namespace together; deleting/rolling back the journal
is not a retry procedure. Checksums are integrity framing, not authentication.

Listings only discover candidate IDs. Once observed, IDs remain in the journal
and their exact paths are polled even if subsequent listings omit them. Missing,
temporarily malformed, reordered or checksum-inconsistent data is retried on
later sweeps. No single absence implies cancellation, non-execution or permission
to reuse an ID. Arbitrarily stale listings can delay discovery of new IDs.
Publishing the marker last and local readback cannot force another NFS/SMB client
to observe cache entries in order; v2 watcher/client exact-path integrity retries
remain necessary. Do not infer global cache coherence from a successful copy.

Hash-verified chunks/artifacts are held until FINAL binds their hashes, so
`--stream` displays output after completion in relayed mode. CLAIMED/RUNNING still
propagate while a command runs. Cancellation remains best effort and cannot undo
side effects. A client timeout prints the same job ID; resume with `fs-exec status`
or `wait`, never submit a replacement because of a timeout.

## Latency, budgets and retention

`health/rally.json` reports p50/p95/p99 for relay-observed outbound/inbound
forwarding (snapshot-read start through verified destination publication), and
known-ping discovery through returned-pong publication. These measurements omit
time before discovery and after drop publication; they are **not true client
end-to-end one-way latency**. Persisted durations use the relay clock and can be
distorted by clock steps across restart. Samples are bounded to 600 total,
200 per statistic, and `sample_retention_seconds` age.

`fs-exec health` probes traverse client → relay → watcher → relay → client. New
client samples measure round trip with one monotonic clock; one-way visibility
figures remain wall-clock estimates requiring synchronized hosts. Fresh client
RTT p99 plus queue/safety margin drives `--transport-timeout auto`; it already
includes both relay legs and is **never added to** the relay estimate. With stale
or fewer than three client samples, a fresh relay recommendation is a floor over
the client fallback. Explicit target/CLI transport timeouts override auto. Relay
recommendations retain the configured conservative fallback, even with samples.
Run at least three probes per client after migration, then periodically; tiny
probes do not predict transfer time for large artifacts. Increase budgets for
bandwidth, queues, fsync and the configured polling maximum.

`max_file_bytes`, `max_publication_bytes`, `max_files` and `max_entries` bound
reads/discovery; `max_pending_bytes` bounds journal payloads; `max_publications`
bounds retained immutable identities. Known IDs are also capped by `max_entries`.
Limits fail closed without publishing the marker. Align them with watcher upload,
output/artifact limits; the example's 16 MiB per-file cap is deliberately stricter
than the watcher's aggregate 128 MiB artifact budget. One publication is buffered
in memory; budget memory for several copies and SQLite overhead. Use disk/inode
quotas on both roots and state, leaving headroom for SQLite's rollback journal
(roughly another pending-byte budget). SQL limits are not a filesystem quota.

Pending publications and identity tombstones have **no time-based expiry**.
Completed journal payload blobs are removed transactionally; identity records
are never evicted to make room. Source transport files are not deleted by rally.
Reaching a retained-count limit requires an operator to increase a justified
budget or drain and retire the whole namespace. Keep sources throughout an
active namespace; removing them yields deferred diagnostics. Never delete
watcher claims while that namespace can accept jobs. SQLite freed pages may
retain prior payload bytes; protect/encrypt local state as sensitive data.

## Directional ACL deployment

Use three distinct accounts: **C** = sandbox client, **R** = outside fs-rally,
**W** = watcher. None should be an administrator or have privileges to change
another account's ACLs. The drop is an intentionally allowed sandbox mount; it
does not grant the sandbox access to the outside share or private relay state.

| Namespace | C | R | W |
|---|---|---|---|
| Drop inbox, cancel, pings | create/write own publications | read/traverse only | none needed |
| Drop results, pongs, heartbeat, rally health | read/traverse only | create/write | none needed |
| Shared inbox, cancel, pings | no access | create/write own publications | read/traverse only |
| Shared results, claims, watcher health/lock | no access | read/traverse only (no lock access needed) | create/write |
| Relay state/config | no access | private state RW; config read-only | no access |

All structural parents must deny untrusted write/delete/rename, DELETE_CHILD,
ACL modification and ownership takeover. In particular, C cannot rename drop
`health`/`results`, and R cannot rename shared `health`/`claims`/`results`. R needs
write on drop `health` to atomically replace its heartbeat copies; C must not
inherit that permission. Protect `health/pings` separately. Do not grant a shared
write group or recursive Modify and assume the program's allowlist fixes it.

On local POSIX filesystems, use the [existing ACL recipe](operations.md#directional-namespace-ownership-is-required)
twice: on the drop substitute R for its watcher/owner and C for its client; on
the real share substitute W for watcher/owner and R for client. Give R read-only
access to C-created drop input children; do not accidentally grant R write to
shared watcher children. Preserve named-user default ACLs and masks with service
umask 0077. The three-UID regression checks that C can read returned results but
cannot overwrite drop FINAL/heartbeat, and R cannot overwrite shared FINAL/CLAIM.

On NFSv4, translate those identities into server-enforced inheritable ACLs, with
stable UID/principal mapping, root squash and denial of WRITE_ACL/WRITE_OWNER and
DELETE_CHILD on protected parents. POSIX `setfacl` text is not an NFSv4 recipe.
On SMB, enforce both share and NTFS DACLs. Remove broad inherited Modify, grant
Read/List/Traverse on incoming-reader and outgoing-reader trees, and creation/write
only to their designated writers. Use UNC paths for services, e.g. TOML literal
`outside_root = '\\server\share\fs-exec'`; disable Offline Files/third-party sync.
Do not grant R admin rights or W's command-execution credentials. Verify effective
create/read/write/delete/rename permissions as **all three identities** on the
actual mounts, including inherited ACLs on newly published files.

## Services, migration and incidents

Linux: install [fs-rally.service](../services/fs-rally.service), edit mount/target
paths and supply mount dependencies. Its filesystem ACLs, not ReadWritePaths
alone, enforce direction. Windows: [windows-rally.ps1](../services/windows-rally.ps1)
is a long-running argument-array launch. Run it in Task Scheduler at startup
(dedicated R account, no execution time limit, restart on failure) or wrap the
exact command using an organization-reviewed WinSW/NSSM configuration. Do not
register plain python.exe with `sc create` as if it implemented the SCM protocol.
Keep secrets out of arguments. Service-manager logs must be rotated and bounded.

Migration from direct mode or upgrades changing mappings/journal format:

1. Stop new submissions. Drain all active and ambiguous jobs in the old target;
   retain old claims and resolve side effects. **No active-job cutover.**
2. Create fresh drop and shared target names and a fresh private relay state.
   Keep the old client registry available for historical status/wait. Do not
   reuse job IDs, copy locks, or seed a new relay from an active direct inbox.
3. Provision/test ACLs, hard links, atomic replacement, persistence and quotas.
   Configure the watcher on the new shared target, R on both new targets, and
   the client registry on the sandbox-visible drop target. Reset that client's
   old direct latency cache (or use a new registry target name).
4. Dry-run, start one R and one W, run three health probes and a disposable
   execution/artifact/cancel test. Test source/destination loss and service restart.
   Switch submissions only after those checks pass. Roll back by draining again,
   never by redirecting a live target or deleting its journal.

For conflicts, quota exhaustion or prolonged DEGRADED health, preserve both
roots and private journal, stop new submissions, and inspect recorded identities.
Do not delete pending rows, FINAL, COMMIT or claims to make the error disappear.
No timeout or cancellation proves that the watcher did not execute. Stop and
verify all previous writers before manual lock or namespace retirement.

## Threat model and release gates

Defends against malformed client traffic, accidental reverse mirroring, partial
copies, stale/reordered visibility, corruption, conflicting immutable records,
unexpected path types and relay process crashes, within the directional ACL and
storage guarantees above. Availability attacks remain possible through filling
the bounded queue, permissions, slow shares or repeated malformed publications.
Every active source is revalidated on sweeps, so retained volume increases I/O;
this is a bounded single-worker transport, not a high-throughput replication
engine. Use conservative timeout budgets and monitor backlog as volume grows.

Hash checks do not authenticate authors. A compromised R can submit requests to
W and forge the drop's result copies; W's policy still limits execution. A
compromised W, storage administrator, kernel or filesystem server is out of scope.
Neither POSIX handles nor Windows handles authenticate a malicious server.
Backing up/restoring only part of a namespace breaks deduplication assumptions.
There is no exactly-once side-effect guarantee, live failover fencing, automatic
lock takeover or command containment added by this sidecar.

Linux local tests and simulated Windows path rules are not Windows/NFS/SMB
certification. Native Windows handles, ACL inheritance, SMB/NFS atomicity/cache
behavior, share disconnects and power-loss durability must pass deployment tests
before production rollout. Python's directory fsync is unavailable on Windows;
NAS/server stable-storage semantics are essential. The existing fs-exec client
and watcher retain their documented Windows limitations. See also the project's
[security model](security.md) and [protocol guarantees](architecture.md).
