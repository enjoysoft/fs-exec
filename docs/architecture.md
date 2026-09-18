# Architecture and protocol

## Components and trust boundary

```text
client / sandbox       NFS, SMB, bind mount       selected target
  fs-exec     ───── immutable marker files ─────▶ fs-exec-watcher
      ▲                                             │ shell=False
      └──── chunks, result, heartbeat, PONG ────────┘
```

The registry maps a name to one target root; no implicit “first watcher” routing exists. Typical entries are the sandbox host, another development Linux machine, production Linux, and native Windows. The client and watcher can see different OS path strings as long as each configuration points to the same share subtree.

The shared filesystem and its ACLs are the transport and authorization boundary. SHA-256 protects against partial/corrupt visibility; it does not authenticate a writer. Run each production watcher as a dedicated least-privilege account and mount only its own per-target directory.

## Layout

```text
TARGET/
├── inbox/JOB.ready/{request.json,uploads/*,COMMIT}
├── claims/JOB/{CLAIM,STARTED}
├── control/cancel/JOB
├── results/JOB/
│   ├── CLAIMED, RUNNING
│   ├── stdout/00000000.chunk, stderr/00000000.chunk
│   ├── artifacts/*, result-UUID.json, FINAL
└── health/{heartbeat.json,watcher.lock/,pings/*.ready,pongs/*}
```

Protocol version 2 uses canonical UTF-8 JSON. IDs and file names contain only ASCII letters, digits, `.`, `_`, and `-`, excluding Windows device names and trailing dots. Requests are assembled under a unique `JOB.UUID.staging` directory. Upload hashes/sizes and request fields are written to `request.json`, then `COMMIT` records its SHA-256, and the directory is renamed to exact `JOB.ready`. The watcher claims before verification but validates bounded request/upload data before launching anything. Immutable-by-convention client files are not an authentication mechanism.

Transport files are written and fsync'd under unique temporary names, then hard-linked atomically to their final names without replacement. This requires same-filesystem atomic hard-link support; unsupported shares fail closed. Heartbeat alone uses replacement. FINAL selects one immutable `result-UUID.json` generation by filename, hash, job ID, watcher ID and fence. A losing publisher cannot overwrite the selected generation. Chunks and artifacts are never overwritten by the watcher. This is a protocol change: upgrade both ends together; do not reuse an active version-1 namespace or delete its claims to migrate.

## State machine and time domains

```text
AWAITING_VISIBILITY → VISIBLE_UNCLAIMED → CLAIMED → RUNNING
                                                  → RESULT_PROPAGATING → COMPLETED
                    any non-final state + stale heartbeat → AVAILABILITY_UNKNOWN
```

Terminal watcher statuses also include `REJECTED`, `FAILED`, `EXPIRED`, `CANCELLED`, `TIMED_OUT`, and `AVAILABILITY_UNKNOWN` after restart. They all have FINAL. FAILED means a post-spawn relay error; the command may already have had effects. If termination itself fails, no terminal result is published and the singleton lock is retained for operator intervention.

Timeouts are intentionally separate:

1. **request visibility / transport** waits for the exact request `COMMIT` path;
2. **queue/claim** waits for exact `CLAIMED`;
3. **command** uses a monotonic deadline immediately before spawning, including surviving children holding output pipes after leader exit;
4. **result visibility** begins when a result generation or FINAL is observed, including incomplete or unreadable contents;
5. **total/tool-call** is a client-side polling budget across all phases (it cannot interrupt a blocked filesystem syscall).

Cancellation and expiry are checked before preparation, between uploads, during approval, before STARTED and again immediately before Popen. Cancellation takes precedence over expiry before launch and over timeout while running. The owner age cap uses the earlier of the client's publication timestamp and the durable watcher-owned claim timestamp; timestamps over five seconds in the future are rejected. Thus future dating cannot extend preparation beyond the owner's cap. This cannot measure a dishonest client's time in an unobserved queue: quotas and retention must bound that storage. Restart preserves the claim rather than resetting its age and executing again. Cancellation remains best effort: a marker can arrive after the last check or remain invisible in a cache, and side effects cannot be undone. A client timeout is an observation failure, not a remote state transition; it returns the job ID and never retries.

## At-most-once claim and recovery

The design uses two levels of exclusive filesystem creation rather than lease takeover:

* `health/watcher.lock` permits one configured watcher per target. No lock is automatically stolen, including a same-host dead-PID lock. An operator must stop and verify the previous watcher and its children before removing one.
* `claims/JOB` is created exclusively and its immutable `CLAIM` includes a random fence token. The watcher checks claim and singleton ownership before transitions, output writes and final publication. STARTED and RUNNING bind job/watcher/fence identity. An existing claim prevents another execution attempt.

Every pre-existing claim without FINAL is conservatively finalized as `AVAILABILITY_UNKNOWN`, even if CLAIM or STARTED is missing, unreadable or delayed. Claims are never released on this evidence. This may sacrifice jobs that never launched, but prevents a missing cached marker from authorizing replay. The optional local JSONL journal is audit data, not protocol authority; journal write failure does not interrupt cleanup.

New namespace ancestor entries, the `claims/JOB` entry in `claims/`, CLAIM, and STARTED are fsync'd before Popen on POSIX. Real fsync errors (including EIO, ENOSPC and EACCES) propagate and prevent launch; only EINVAL/ENOTSUP directory-fsync errors are treated as unsupported. Python has no portable Windows directory flush here. Unsupported directory fsync, NAS volatile caches, server rollback or removal of claims invalidate power-loss duplicate-suppression guarantees. These are deployment properties, not proven by local tests.

The token checks are fail-closed local ownership checks, **not server-enforced distributed fencing**. Cached reads cannot revoke a live remote writer; first-FINAL-wins protects data from overwrite but cannot order two live watchers or stop their external side effects. Cross-host automatic failover is unsupported. Do not remove a lock until all old writers are stopped. This design assumes tested server-atomic mkdir, hard-link and same-filesystem rename, retained durable claims, and directional ACLs; it does not promise exactly-once effects.

## Polling, delayed visibility, and ordering

Directory scans discover candidates; validation uses exact paths. No filesystem notification is required. Phase waits use jittered backoff; result/output/PONG waits poll exact paths. Missing, unreadable, unparsable or checksum-mismatched FINAL and result generations remain `RESULT_PROPAGATING`. Final chunks and artifact ancestors/content are retried within a bounded propagation budget; persistent bad data never produces verified success. Request verification likewise retries missing or checksum-inconsistent data, but rejects malformed identities and exceeded resource limits. This tolerates tested delayed-visibility scenarios, not arbitrary server inconsistency.

Stdout and stderr are independent numbered streams; cross-stream ordering is not claimed. Live `--stream` output is **provisional and unverified**, and must not drive automated side effects. FINAL binds each chunk's hash and byte count. `wait` validates final streams even without `--stream`; it also compares hashes of already-emitted bytes and fails on disagreement (it cannot retract displayed bytes). Reading after FINAL verifies each chunk before emission. A bounded producer queue, byte cap and 4096-chunk-per-stream cap bound output. Artifact copies stop at their byte budget even when a source grows; count and bounded-glob restrictions also apply. Downloads validate a private temporary copy and install it without overwriting existing destinations.

## Latency and automatic overhead

`health` publishes a staged PING and polls its exact PONG, then rereads heartbeat. A job-scoped thread refreshes BUSY heartbeats and services pings during verification, hashing, approval, execution and artifact copying. Failed heartbeat writes stop approval/execution at their next check; an uninterruptible filesystem call can still block progress and delay command termination beyond its budget. With synchronized clocks, watcher `observed_ns` estimates request visibility and `responded_ns` to client receipt estimates result visibility. The local sample file exposes request/result/round-trip p50, p95, p99, age, and count.

Automatic overhead is:

```text
request p99 + result p99 + 2s queue margin + 2s safety margin
```

With fewer than three samples or samples older than 15 minutes, it conservatively uses 30 seconds. A target can override this. One-way values are only estimates when clocks are skewed; use NTP and treat round-trip as the reliable bound.

## Command model

Argument mode resolves only owner-configured absolute executables and passes an array to `Popen(..., shell=False)`. POSIX passes argv directly; Python serializes argv using Windows quoting rules on Windows, whose target programs may parse differently. Batch `.cmd`/`.bat` entry points are denied. Explicit Bash disables profile/rc loading; PowerShell is noninteractive with `-NoProfile`. A runtime must also have its executable allowlisted.

Inline stdin is file-backed to avoid deadlocks and broken pipes. Both commands and approval hooks own a new POSIX session and always run group termination/direct-child reaping in `finally`, before a terminal result. POSIX cleanup uses SIGKILL on the original group ID even after its leader exits, including TERM-ignoring members. Windows uses bounded `taskkill /T /F` plus direct-child kill/wait; it has no Job Object and cannot guarantee containment after leader exit. Detached POSIX descendants and native Windows behavior require stronger OS containment and deployment tests.
