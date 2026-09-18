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
│   ├── artifacts/*, result.json, FINAL
└── health/{heartbeat.json,watcher.lock/,pings/*.ready,pongs/*}
```

Protocol version 1 uses canonical UTF-8 JSON. IDs and file names contain only ASCII letters, digits, `.`, `_`, and `-`. Requests are assembled under a unique `JOB.UUID.staging` directory. Upload hashes/sizes and request fields are written to `request.json`, then an immutable exclusive-create `COMMIT` records its SHA-256, and the directory is renamed to exact `JOB.ready`. The watcher ignores staging directories and verifies every hash before claiming execution. Results use atomic chunk writes and an atomic `result.json`; immutable `FINAL` contains the result hash. Readers trust nothing before `FINAL` and verify it afterward.

## State machine and time domains

```text
AWAITING_VISIBILITY → VISIBLE_UNCLAIMED → CLAIMED → RUNNING
                                                  → RESULT_PROPAGATING → COMPLETED
                    any non-final state + stale heartbeat → AVAILABILITY_UNKNOWN
```

Terminal watcher statuses also include `REJECTED`, `EXPIRED`, `CANCELLED`, `TIMED_OUT`, and `AVAILABILITY_UNKNOWN` after restart. They all have `FINAL`.

Timeouts are intentionally separate:

1. **request visibility / transport** waits for the exact request `COMMIT` path;
2. **queue/claim** waits for exact `CLAIMED`;
3. **command** begins when the watcher writes `STARTED`, immediately before spawning;
4. **result visibility** begins when `result.json` is visible but `FINAL` is not;
5. **total/tool-call** is a client-side hard wall across all phases.

Expiry is wall-clock durable and checked before execution. Command runtime uses a monotonic clock. A client timeout is an observation failure, not a remote state transition; it returns the job ID and never retries.

## At-most-once claim and recovery

The design uses two levels of exclusive filesystem creation rather than lease takeover:

* `health/watcher.lock` permits one configured watcher per target. A restart may reclaim only a same-host lock whose recorded PID is dead. Cross-host locks are never stolen automatically; an operator must verify the previous target process is dead before removing one.
* `claims/JOB` is created atomically and its immutable `CLAIM` includes a random fence token. Duplicate directory observations cannot execute an already claimed job.

`STARTED` is the replay boundary. On restart, a started claim without `FINAL` is finalized as `AVAILABILITY_UNKNOWN`; the command may have had effects, so it is not run again. After singleton ownership transfers, an unstarted claim is safely released because `STARTED` is durably written before `Popen`. This favors duplicate prevention over automatic availability. The optional fsync'd local JSONL journal is audit data, not protocol authority.

This assumes same-server atomic rename and exclusive directory creation for clients of the selected share. Validate those semantics on the actual NAS/file server before production use.

## Polling, delayed visibility, and ordering

Directory scans discover candidate queue names, but every consequential transition polls or verifies an exact marker path. No filesystem notification is required. Clients use jittered exponential backoff for exact PING/PONG and phase marker waits; status/output waits poll exact numbered chunks and markers. Delayed listings may postpone work but cannot publish partial work. Reordered visibility is tolerated because COMMIT/FINAL contents bind hashes and identities. Missing or unreadable markers are “not yet observed”, not cancellation or failure.

Stdout and stderr are independent numbered streams; ordering between the two streams is not claimed. A bounded producer queue prevents RAM growth if the share is slow, and the total output cap stops disk growth. Artifacts are files matching cwd-relative globs and are checksum-verified on download.

## Latency and automatic overhead

`health` publishes a staged PING and polls its exact PONG. With synchronized client/target clocks, watcher `observed_ns` estimates request visibility and `responded_ns` to client receipt estimates result visibility. The local append-only sample file retains recent observations and exposes request/result/round-trip p50, p95, p99, age, and count.

Automatic overhead is:

```text
request p99 + result p99 + 2s queue margin + 2s safety margin
```

With fewer than three samples or samples older than 15 minutes, it conservatively uses 30 seconds. A target can override this. One-way values are only estimates when clocks are skewed; use NTP and treat round-trip as the reliable bound.

## Command model

Argument mode passes an exact string array to `subprocess.Popen(..., shell=False)`. No quoting round trip occurs. Shell mode is explicit and maps to `bash -euo pipefail -c SCRIPT` or noninteractive PowerShell. The target platform controls the executable (`powershell.exe` on Windows, `pwsh` elsewhere). Cancellation and timeouts terminate the process group/tree (POSIX session or Windows `taskkill /T`).
