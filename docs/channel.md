# fs-channel: durable group chat and control

`fs-channel` adds a filesystem-backed group channel to the same dependency-free
Python 3.11 package as fs-exec and fs-rally. One Amp coordinator can assign work
to four Claude Code/Grok agents, and all five receive the same transcript.
The broker **never runs an agent, shell, model API, or task**. An owner-configured
adapter gives an external agent the channel as tools; that agent's own execution
policy still applies. A message is untrusted data, not authority to run its text.

Use a separate namespace from fs-exec targets. Existing fs-rally remains the
fixed protocol-v2 execution transport: it does not recursively copy this channel.
The channel broker itself reads each configured participant inbox and writes each
participant feed. Its mailboxes may be on different allowed drops/mounts, so the
broker can run outside a sandbox and perform the directional fanout directly.
At least one authorized filesystem/process channel must cross each boundary.
This is not a sandbox escape, an authentication bypass or a general sync tool.

## Provision one broker and five identities

1. Install with `python -m pip install .`. Copy [channel.toml](../examples/channel.toml).
   Choose a unique channel name and new, absolute, non-overlapping mailbox roots.
   Put broker state on **private local durable storage**, not NFS/SMB. Each client
   also needs its own private local state outside all mailboxes and broker state.
2. An administrator runs `fs-channel --config channel.toml init` and applies the
   directional ACLs below **before starting participants**. Init creates `CHANNEL`,
   `inbox`, and `feed`; it does not set ACLs or certify a mount. Config and CHANNEL
   are administrator-owned read-only files. Agents cannot provision namespaces.
3. Run exactly one broker for the namespace:
   `fs-channel --config channel.toml run --poll 0.2`.
   One client state/adapter owns each identity; concurrent use of that same
   state fails its local OS lock. Do not run a second client state for an author.
4. Give Amp `amp` credentials; give each bot only its own filesystem principal,
   config and client state. Paths may be translated in agent-local configs for
   mounts, but channel, participant order, roles and budgets must match CHANNEL.
   Config paths' structural ancestors must be traversable for path validation;
   no access to other agents' inbox contents or private state is needed.

For Linux supervision use [fs-channel.service](../services/fs-channel.service)
with actual mount dependencies and paths. Rotate service logs. On Windows use
Task Scheduler at startup (dedicated broker account, restart on failure, no run
time limit) or an organization-reviewed service wrapper around
`python -m fs_exec.channel --config C:\\ProgramData\\fs-exec\\channel.toml run`.
Use UNC paths, not session-local mapped drives. Plain Python is not an SCM service.

## CLI and adapters

Global `--config` precedes the subcommand. Output is JSON; `receive` emits JSONL.
Exit 0 means the operation succeeded, **not that an external task executed**;
`once` returns 1 for deferred ingress, 2 for errors, 130 for interruption.
`run` reports degraded output/storage errors and retries. `status` reads private
broker state under its lock, so stop the broker first; ordinary agents consume
the feed, not broker state. `once` is one bounded round, not a queue drain.

```sh
fs-channel --config channel.toml send --participant amp --state /private/amp \
  --id assign-review-one \
  --message '{"kind":"assign","task":"review-one","assignee":"claude-one","text":"Review the parser; report findings, do not deploy."}'
fs-channel --config channel.toml receive --participant claude-one --after 0
fs-channel --config channel.toml send --participant claude-one --state /private/claude-one \
  --id started-review-one \
  --message '{"kind":"status","task":"review-one","revision":1,"status":"running","text":"Review started"}'
fs-channel --config channel.toml pending --participant claude-one --state /private/claude-one
fs-channel --config channel.toml sync --participant claude-one --state /private/claude-one
fs-channel --config channel.toml status
```

The example's revision 1 is valid only if the assignment was event 1. Always use
the actual last accepted revision, not the example's constant. All identifiers
are lowercase ASCII `[a-z][a-z0-9_-]{0,47}`, excluding Windows reserved names.

**Retry with the same `--id` and identical message**, even if no response arrived.
The client commits the ID and bytes before touching the mailbox and prints its
author sequence before transport. `sync` resumes only existing queued records;
it never allocates new identities. Same ID/different message fails. Successful
publication removes only the private pending payload, retaining the ID/hash and
sequence forever. A published input is not an accepted event: look for the
matching `(author, author_seq)` in the feed and check `accepted` and `reason`.

### Claude Code / Amp shell-tool adapter

Configure fixed participant/config/state arguments outside model control. Expose
`receive`, `send`, `pending` and `sync` through the agent's authorized command/tool
interface. No Claude-specific SDK or internal transcript format is required.
For JSON stdin instead of command-line message text:

```sh
printf '%s' '{"id":"greeting-one","message":{"kind":"chat","text":"Parser review is ready","mentions":["amp"],"reply_to":1}}' |
  fs-channel --config channel.toml adapter --participant claude-one \
    --state /private/claude-one --after 0
```

This adapter accepts one JSON object up to `max_message_bytes`, then EOF. `{}`
only synchronizes pending messages and reads a page. `{id,message}` also sends
one idempotent message. Output has two JSON lines: `{queued:[author_seq]}` then
`{events:[...]}`. A failure after the first line still leaves a recoverable ID.
This is a bounded request/response adapter, **not MCP or an always-running bot**.

### Grok / other tool-calling API adapter

Use the same JSON adapter as an owner-controlled tool, with argument-array
execution (`shell=False`) and fixed executable/config/identity paths. Or import
the Python API directly in the bot's tool handler:

```python
from pathlib import Path
from fs_exec.channel import ChannelConfig, Client, read_events

config = ChannelConfig.load(Path("/etc/fs-exec/channel.toml"))

def channel_send(id: str, message: dict) -> dict:
    with Client(config, "grok-one", Path("/private/grok-one")) as client:
        seq = client.queue(message, id)
        client.sync()
        return {"author_seq": seq}

def channel_receive(after: int) -> list[dict]:
    return read_events(config, "grok-one", after)
```

Register those handlers with the bot framework's function tools; its provider
credentials remain outside the channel/broker. Persist an application operation
ID **before** making a send/tool call and reuse it on retry, including across
model turns. A provider generating a fresh tool-call ID on retry is not a stable
application operation ID. This integration has no dependency on vendor CLI flags
or SDK versions; live Claude/Grok sessions and paid API calls are not part of tests.

Adapters persist their **last processed** global sequence only after handling a
page. Repeated delivery after a crash is expected; deduplicate by `(channel,seq)`.
Never advance the cursor on a read error or merely on HEAD. Explicit `ack` means
the application says it processed everything through `upto`; it is not automatic
delivery confirmation, evidence of execution, or permission to delete history.
For tasks with side effects, keep a separate durable execution ledger keyed by
`(channel,task)`, commit intent before starting work, and treat a surviving intent
after crash as ambiguous. Do not automatically execute an assignment twice. The
channel does not solve exactly-once external effects. An adapter may invoke the
existing fs-exec client under its existing policy, retaining and resuming that
job ID; channel messages cannot expand watcher policy or choose executables.

## Message contracts and order

Every message object has exactly the listed fields (including explicit nulls).
Text must be a nonempty string; the encoded request fits `max_message_bytes`.

| kind | Additional fields | Meaning |
|---|---|---|
| `chat` | `text`, `mentions` (unique participant names), `reply_to` (earlier global seq or null) | Broadcast to all, with explicit attention/thread metadata. Mentions are not private routing. |
| `ack` | `upto` (0..previous global seq) | Monotonic per-author application acknowledgement. |
| `presence` | `session` (identifier), `ttl` (0..max_lease_seconds) | Receipt-time lease; zero withdraws presence. |
| `assign` | `task`, `assignee`, `text` | Coordinator only; task ID cannot be reused or reassigned. |
| `status` | `task`, `revision`, `status`, `text` | Assignee updates with current global revision. Coordinator may only cancel another assignee's task. |

Tasks start `assigned`; allowed transitions are `assigned → running/blocked/cancelled`,
`running → blocked/completed/failed/cancelled`, `blocked → running/failed/cancelled`.
Terminal states are immutable. Each accepted assignment/status sets the revision
to its event sequence. Stale concurrent updates become rejection events without
changing task state. Cancellation is a control message, not a process kill or
proof that effects stopped. Explicitly resolve an old task before assigning new
work under a new ID. There is no automatic lease-driven retry or reassignment.

Presence expires at broker receipt time plus TTL, using its wall clock so restart
preserves expiry. Delayed heartbeats can appear fresh and clock changes distort
expiry. `session` identifies a process incarnation for observers; it is **not a
fencing token** and competing sessions under one OS identity are not isolated.
Use presence only as a hint, never to infer nonexecution or authorize takeover.

The broker reads each participant's exact next 16-digit sequence path once per
round, in configured participant order. It never enumerates inbox contents. A
missing/invalid frame holds only that author's next slot; other authors proceed.
Global order is **broker admission order**, not timestamps or simultaneous send
order. Author sequences are contiguous; late visibility cannot reorder an author.
A semantically invalid but correctly framed request gets a durable rejected event
and consumes its author slot. Framing/identity/size failures remain deferred for
operator inspection; never renumber or skip a missing slot to recover.

## Publication and crash recovery

Mailbox layout: administrator `CHANNEL`; author-written `inbox/NNNNNNNNNNNNNNNN.json`;
broker-written `feed/NNNNNNNNNNNNNNNN.json` and mutable `feed/HEAD`.
The author is derived from the configured inbox, not trusted from input JSON.
Both must agree. No participant can submit on behalf of another with proper ACLs.

1. The client journals bytes, stable idempotency key and next sequence in one
   FULL-synchronous SQLite transaction. Complete files are atomically hard-linked
   without replacement. Stable bounded reads reject special files, symlinks,
   reparse points, unexpected hard links, sparse files and observed mutation.
2. The broker validates one request and commits its author high-water mark,
   semantic state, global sequence and exact envelope in **one transaction**.
   A rejected semantic operation rolls back its state changes but records rejection.
3. Journal bytes replay to every feed without replacing existing events, with
   readback and directory fsync through each mailbox root even on equal-file retries.
   Only then is HEAD advanced on every mailbox, and the journal marks publication
   complete. An output failure halts all new intake until fanout recovers.
4. Recovery replays the pending envelope **before any new input**. Changing its
   source cannot change accepted bytes. It does not replay completed events or
   resurrect removed publications. Retained sequence high-water marks prevent
   re-admission of already-consumed input slots.

Each envelope contains an event, its SHA-256 and the preceding event hash. HEAD
binds the latest sequence/hash. Readers fetch a bounded consecutive exact-path
page and validate the chain; if HEAD is ahead of visible files, or a cursor is
ahead of stale HEAD, they fail and preserve the cursor for retry. Partial fanout
can expose different prefixes on different clients. A successful fsync/readback
does not force NFS/SMB cache coherence or simultaneous delivery. Hashes are not
signatures: a compromised broker can forge the transcript.

Keep broker journal, all mailboxes and all client journals together for the life
of the namespace. Never delete/roll back journals, mutate committed events, reuse
IDs or start with a fresh state directory as a retry procedure. Missing/corrupt
completed files require incident recovery from a consistent backup, not replay.
Private SQLite freed pages may retain payloads; protect/encrypt state and backups.
The OS lock excludes local processes using one state directory; it is **not
distributed election/fencing**. Only one broker may use a namespace, even across
hosts/different state paths. Stop and verify old writers before maintenance.

## Bounded resources and retention

`max_events` caps the entire namespace, **including rejected messages, acks and
presence**. Journal envelopes and task IDs stay retained. Each client permits at
most `max_events` distinct send keys and `max_pending` unpublished local requests;
already published but unprocessed inputs remain until retirement. No identity is
evicted to make room, and no active namespace files are automatically deleted.
Read pages are capped by `page_size`. Broker work per round is one bounded request
per participant plus at most one pending fanout. Slow filesystem syscalls can
still block a round; no hard syscall deadline is claimed.

Budget disk for roughly `max_events × (max_message_bytes + 4096)` per feed and
broker event journal, up to `max_events × max_message_bytes` per author inbox,
SQLite indexes/rollback journals, and temporary publication files. A page can
hold `page_size` envelopes in memory. Limits are not quotas: enforce disk/inode
quotas on every mailbox and private store to bound malicious inputs and temp
files left by repeated process deaths. Monitor and rotate service logs separately.

Reserve capacity for finishing tasks, acks and shutdown before the hard cap.
At the cap intake fails closed; presence no longer renews and tasks cannot change
state. Stop new work **well before** that point. Drain tasks, resolve ambiguous
execution externally, obtain required application acknowledgements, stop all
writers, archive the whole namespace consistently, then provision a new channel
name/roots/journals with reviewed budgets/roles. Delete only a fully retired
namespace according to the operator's retention policy. A cap does not justify
deleting events, IDs or execution claims. In-place membership/config changes and
active-task migration are deliberately unsupported.

## Directional ACL deployment

Use six unprivileged principals: broker B and five participants P. The Amp
coordinator is a participant, **not the broker account**.

| Namespace | Owning participant P | Other participants | B |
|---|---|---|---|
| P's inbox | create/read/write own files | none | read/traverse only |
| P's feed | read/traverse only | none | create/read/write |
| P's CHANNEL and structural parents | read/traverse only | minimal structural traversal if needed | read/traverse only |
| P's private local state | private RW | none | none |
| B's private local state | none | none | private RW |
| Config/ACL administration | read-only | read-only as needed | read-only |

Structural parents must deny untrusted write/delete/rename, DELETE_CHILD,
WRITE_ACL, WRITE_OWNER and ownership takeover. **Do not give a participant
recursive Modify or a shared write group**. B cannot rename inbox/feed or forge
participant input; P cannot overwrite feed/HEAD or another author's input.
On POSIX, provision structural parents under an administrator account, give B
and P traverse rights, and use named-user access/default ACLs:

```text
# Illustrative per-mailbox ACLs; set owners/parents first, substitute real users.
inbox: u::rwx,u:P:rwx,u:B:r-x,g::---,m::rwx,o::---
inbox default: u::rwx,u:P:rwx,u:B:r-x,g::---,m::rwx,o::---
feed: u::rwx,u:P:r-x,u:B:rwx,g::---,m::rwx,o::---
feed default: u::rwx,u:P:r-x,u:B:rwx,g::---,m::rwx,o::---
```

Shared files request mode 0660 so named-reader ACLs survive a service umask of
0077. Ensure newly created inbox files give B read but not write, and feed files
give P read but not write. New private local state directories are 0700 on POSIX;
pre-existing private state directories and Windows DACLs are the operator's job.
Use the [execution ACL guidance](operations.md#directional-namespace-ownership-is-required)
for ownership/default-mask principles, not a recursive application across roots.

NFSv4 requires server-enforced inheritable ACLs, stable principal/UID mappings and
root squash; POSIX ACL text is not a portable NFSv4 recipe. SMB requires both
share and NTFS DACLs, removal of inherited broad Modify, and no Offline Files or
third-party sync. Test effective create/read/write/delete/rename privileges under
**every principal**, including newly created children and both directions. Test
atomic hard links, loss/disconnect, stale visibility and restart on actual mounts.

## Threat model and retained limitations

Defends against malformed input, author spoofing across isolated inboxes, partial
publication, duplicates with retained state, stale/reordered visibility, bounded
resource consumption by honest peers, and process crashes under tested storage
semantics. A participant can corrupt or withhold its own slot, spam up to quotas,
lie in task/ack/presence messages, and consume the finite event budget. A failed
mailbox blocks global fanout. All channel members receive all accepted messages;
there are no private messages, encryption, cryptographic author signatures or
content/prompt-injection filtering. Do not send secrets to the group.

Windows source reads reuse fs-rally's handle checks; destination paths still
require trusted ACLs. Python has no portable Windows directory fsync, and some
POSIX shares report unsupported directory flushes; physical durability then
depends on the server. Native Windows/NFS/SMB behavior, power loss, provider SDKs
and real external-agent execution require deployment testing. There is no
distributed fencing, safe live failover, automatic bot launch, automatic task
execution, or exactly-once side effects. Existing fs-exec/fs-rally execution,
policy, claims and cancellation guarantees are unchanged.
