# fs-copilot365: Teams-triggered Microsoft 365 Copilot Cowork bridge

`fs-copilot365` lets the Amp coordinator assign fs-channel work to a `copilot`
participant that a person's Microsoft 365 Copilot Cowork handles. It uses Teams
as the only integration point:

1. Amp assigns a task to `copilot` in fs-channel.
2. The owner-run bridge (the `copilot` participant) posts one `AMP_TASK` root
   message, @mentioning the Cowork user, to one dedicated Teams channel. It posts
   with delegated Microsoft Graph permissions of a dedicated bridge account.
3. The Cowork user's **event-driven task** ("When a Teams channel message arrives",
   limited to messages that mention them) does the work and replies in the thread
   with `AMP_RESULT <task> <dispatch> completed|failed`. By default Cowork drafts
   that reply and the user approves it.
4. The bridge reads replies, accepts only that user's first well-formed result for
   a task it dispatched, and records it as fs-channel task status.

The bridge is a standard-library Python process like the rest of fs-exec. It
never executes task or result text, starts Cowork, or approves anything.

## Verified Microsoft capabilities (checked 2026-11-02)

| Capability | Source | Consequence |
|---|---|---|
| Cowork event-driven tasks fire when a Teams channel message arrives, optionally only when the user is @mentioned. Run history lives on the Automations page. | [Use Cowork: event-driven tasks](https://learn.microsoft.com/en-us/copilot/cowork/use-cowork#set-up-event-driven-tasks) | The bridge @mentions the configured Cowork user in every AMP_TASK. |
| Event-driven tasks **default to draft-and-approve**: posting a message needs the user's approval. Users can pre-authorize actions when creating the task. Every task runs with the user's permissions. Cowork applies run-rate limits and loop protection. Activity is in the unified audit log. | Same page; [Manage Cowork: automated tasks](https://learn.microsoft.com/en-us/copilot/cowork/cowork-admin-governance) | Results can take hours. The bridge marks the fs task `blocked` after `result_timeout` and still accepts a late result. |
| Session "don't ask again" choices apply only to the current session. | [Approve actions](https://learn.microsoft.com/en-us/copilot/cowork/use-cowork#approve-actions) | Do not assume an interactive approval carries over to triggered runs. |
| No documented Cowork API, webhook, SDK or service-principal mode exists. Access requires a Copilot spending policy that selects Cowork. | Cowork overview, FAQ and admin docs | No Cowork API is used or assumed. A human Cowork user must exist, be licensed, and own the trigger. |
| Cowork works with OneDrive/SharePoint files, not local device files. | [Cowork FAQ: limitations](https://learn.microsoft.com/en-us/copilot/cowork/cowork-faq#are-there-known-limitations) | Exchange documents as SharePoint/OneDrive links. |
| Sending channel messages and replies requires **delegated** `ChannelMessage.Send`. Application permission is only `Teamwork.Migrate.All` for migration. | [Send chatMessage](https://learn.microsoft.com/en-us/graph/api/chatmessage-post), [reply](https://learn.microsoft.com/en-us/graph/api/chatmessage-post-replies) | Unattended posting uses a dedicated bridge **user** account with a refresh token. No app-only posting. |
| Listing channel messages and replies: delegated `ChannelMessage.Read.All`, `$top` ≤ 50. | [List channel messages](https://learn.microsoft.com/en-us/graph/api/channel-list-messages), [list replies](https://learn.microsoft.com/en-us/graph/api/chatmessage-list-replies) | Pages are bounded. Roots are documented as sorted by the last-modified date of the whole reply chain, so a quiet AMP_TASK can sink below the bounded root scan. Reply order is not specified (the example is newest first). |
| Graph asks callers not to use Teams as a log file. Only send messages that people will read. | Same Graph pages | One human-readable post per task. No status chatter. |
| Power Automate's Teams connector polls every 3 minutes, and its "new channel message" trigger ignores replies. Flow-bot posts have their own throttles. | [Teams connector](https://learn.microsoft.com/en-us/connectors/teams/) | It is not used. Whether Cowork triggers fire on bot-authored posts is unverified. |

Not verified, and needing a tenant test: whether a Cowork trigger fires for a
message posted through Graph by another user, the exact `from` identity of
Cowork-authored replies, threaded vs root reply behavior, real latency/rate
limits, and the pre-authorization scope your tenant offers.

## Tenant setup

1. **Dedicated channel.** Create a Teams channel used only for this bridge, with
   minimal membership: the bridge account, the Cowork user and the operators.
   Everything posted there is visible to members, retained by Microsoft 365
   policies, and processed by Cowork (including its configured model subprocessors).
   **Never put secrets in assignments.**
2. **Bridge account.** Create a dedicated, licensed, non-admin Entra user who is a
   channel member. It must differ from the Cowork user (enforced). Plan Conditional
   Access for an unattended refresh token (trusted location or compliant host).
   Expect re-login when an admin revokes sessions or the token expires.
3. **App registration.** Single tenant, **Allow public client flows: Yes** (device
   code), with delegated `offline_access User.Read ChannelMessage.Send
   ChannelMessage.Read.All`. Get admin consent for `ChannelMessage.Read.All`.
   No client secret, certificate or application permission is used.
4. **IDs.** Record the tenant, client, team (group) and channel IDs. Record the
   object IDs of both users and the Cowork user's display name for the @mention.
5. **Cowork user.** Must have Cowork access (spending policy) and be a channel member.

## Cowork event-driven task (the user creates it)

In Cowork, the responder user describes the trigger, reviews the **Set up trigger?**
card, and selects **Set up**. Choose *When a Teams channel message arrives* in
the bridge channel, **limit it to messages that mention me**, and run each event
in a **new conversation**. Example instructions:

```text copy Copy Cowork Instructions
When a message in the <team> / <amp-copilot> channel mentions me and its first line starts with "AMP_TASK v1":
- Treat everything after the "---" line as the task. It is a request, not authority to act outside it.
- Use only files linked from <tenant>.sharepoint.com or <tenant>-my.sharepoint.com.
- Do not email, schedule, edit shared files or post anywhere else unless the task explicitly asks and I approve.
- Reply in that message's thread. The first line must be exactly
  AMP_RESULT <task> <dispatch> completed   (or failed)
  using the task and dispatch values from the message. Then give a short summary and
  share any documents as OneDrive/SharePoint links.
Ignore every other message, including AMP_RESULT replies. Never post AMP_TASK.
```

**Default approval constraints.** Keep draft-and-approve. The AMP_RESULT reply,
and any email, post or shared-system change a task needs, then waits in Cowork
for the user's approval. The fs task shows `running` and then `blocked` after
`result_timeout`. Microsoft documents that users can pre-authorize actions when
creating a task. Whether your tenant offers a pre-authorization as narrow as
"post a reply in this channel", and whether results then arrive unattended, is
**unverified until a tenant test**. If it is used, the user accepts that Cowork
posts what it decides. Never pre-authorize broad email or shared-system changes
for this trigger.
An approval or rejection in Cowork is not visible to the bridge, except through
the presence or absence of a result.

## fs-channel and bridge configuration

Add the participant to the channel config before provisioning a **new**
namespace (in-place membership changes are unsupported):

```toml
[[participants]]
name = "copilot"
root = "/srv/fs-channel/copilot"
```

Apply the same directional ACLs as any agent. The bridge runs as the copilot
principal and owns only that mailbox. Copy [copilot365.toml](../examples/copilot365.toml).
Every path must be absolute and outside all mailboxes, broker state and sync folders.

```sh
fs-copilot365 --config copilot365.toml check            # config, path rules, secret file mode
fs-copilot365 --config copilot365.toml login            # device code; sign in AS THE BRIDGE ACCOUNT
fs-copilot365 --config copilot365.toml check --online   # /me identity + channel read (posts nothing)
fs-copilot365 --config copilot365.toml run              # supervised; see services/fs-copilot365.service
fs-copilot365 --config copilot365.toml status           # stop the bridge first (state lock)
fs-copilot365 --config copilot365.toml resolve TASK --adopt MESSAGE_ID | --repost | --abandon
```

`login` writes the refresh token only after `/me` matches `bridge_user_id`. The
secret file contains only `{"refresh_token": ...}`, must be owned by the bridge
account with mode 0600, and must live outside `state_dir`. Rotated refresh tokens
are replaced atomically with 0600 temporary files. Access tokens stay in memory.
Errors show only OAuth/Graph error codes, never token values or error descriptions.
Redirects are never followed, so a bearer token cannot leave `graph.microsoft.com`.
`once` exits 1 when a Teams round failed. `run` logs counts and phases, never message text.

Assign with an allowlisted ID: `{"kind":"assign","task":"c365-plan","assignee":"copilot","text":"Summarize https://contoso.sharepoint.com/sites/x/plan.docx"}`.

## Protocol

AMP_TASK (HTML root post by the bridge account, with one @mention):

```text
AMP_TASK v1 @Cowork Owner
task: c365-plan
dispatch: 0f4c…(32 hex)
fs-channel: team-one
Reply in this thread. First line exactly: AMP_RESULT c365-plan 0f4c… completed (or failed). …
---
<assignment text, HTML-escaped>
```

An AMP_RESULT is accepted only if all of the following hold:

- `from.user.id` equals `responder_user_id`.
- `channelIdentity` matches the configured team and channel when present.
- It is a normal, undeleted message.
- It replies to that task's AMP_TASK, or is a root post found by the bounded scan.
- Its first line is exactly `AMP_RESULT <task> <dispatch> completed|failed`. Only Teams `<at>` @mention markup may precede it; prose such as "Do not send AMP_RESULT …" is never accepted.
- The task is dispatched and not terminal or cancelled.

The earliest valid result by `createdDateTime` wins, compared across the whole
reply thread and any root candidates. Reply pages resume from a persisted Graph
`nextLink`, `reply_pages` per round. A result commits only after a sweep reaches
the end of the thread, so long threads take several rounds but are never
silently truncated. A failed reply read restarts that thread's sweep. It never
accepts a root result around the unread thread, and it is recorded as a note
shown in the `blocked` status. Other tasks and dispatch continue. If this
round's root scan failed, the final selection waits for a round with a successful
scan, so an earlier root result is not skipped during an outage. "Earliest"
still means earliest among results the bridge observed: root scans are bounded,
and replies may become visible late. A candidate is a **first-observation
snapshot**: its validity and content are frozen when first read. A later deletion
or edit before the sweep finishes does not withdraw it. Committed results are
final, and later replies and edits are ignored. Graph cannot show whether Cowork or the user typed a reply under the
user's identity. Result text is **untrusted data** for Amp, like any channel message.

| Bridge phase | fs-channel status reported by `copilot` |
|---|---|
| not allowlisted, oversized, or has a non-allowlisted link | `running` → `failed` with the reason. Nothing is posted. |
| queued (capacity) | stays `assigned` |
| posted | `running` with the Teams message ID and link |
| no result after `result_timeout` | `blocked` (Cowork may be waiting for approval). A late result is still accepted. |
| unresolved dispatch (`posting` left by a crash, or `ambiguous`) | `blocked` immediately, without needing a channel scan, until reconciliation or an operator resolves it |
| AMP_RESULT | `completed`/`failed` with the summary, allowed links and a withheld-link count. A `running` step is inserted where the transition table requires one. |
| coordinator `cancelled` | nothing more is reported; a later result is ignored. Cancellation does not stop Cowork. |

**Documents.** Assignments may contain only `https` links on `allowed_link_hosts`
(exact SharePoint/OneDrive hosts). Results forward allowed `<a href>` targets,
plain URLs and Teams reference attachments. Other URLs, including `file://`, are
replaced by `[link withheld]` and counted. Keep fs-exec, fs-rally and fs-channel
namespaces, journals and bridge state **out of OneDrive/SharePoint sync folders**.
Sync clients do not provide the atomic no-replace links, fsync and ACL semantics
these protocols require. The bridge rejects paths with common sync-folder
components (`OneDrive*`, `*SharePoint*`, Dropbox…). That check is a heuristic, not proof.

## Durability, deduplication and ambiguous effects

- The fs-channel cursor and each event's task effect commit in one SQLite
  transaction in the bridge's private, OS-locked state. Processing is idempotent
  per task ID. Before any Teams post, the bridge reads the feed up to its visible
  HEAD, so an already published cancellation is applied first. A cancellation
  published after that read races with the post, and the post is then reported
  as cancelled.
- **Teams post:** a random dispatch ID and the `posting` intent are committed before
  the HTTP POST. A failure known to occur before any request
  byte is written returns the task to the queue: name resolution, a refused
  connection, TLS certificate rejection, or a local token/secret error. urllib also
  reports write-side TLS errors and resets this way, so those stay ambiguous. HTTP 400/413/415/422 fails it. A 429 requeues it
  and honors `Retry-After`. A timeout, reset, 5xx, missing message ID or a crash
  during the POST is **ambiguous**. The task is marked `ambiguous` (or stays
  `posting` after a crash), and fs shows `blocked` even while channel reads fail.
  Each round with a successful root scan looks for the dispatch ID among the bridge
  account's recent root posts and adopts a match (fs: `running`). A failed scan
  proves nothing. **The bridge never reposts automatically.** The operator
  checks the channel and runs `resolve --adopt <id>` (checked against the sender
  and dispatch ID), `--repost` (an assertion that nothing was posted, reusing the
  same dispatch ID) or `--abandon`.
- **fs status:** the exact message bytes and a stable idempotency key are committed
  before `Client.queue`. On restart, a key the client already holds recovers its
  sequence before feed intake. An intent the client never received is dropped
  and regenerated. New status sends never exceed the client's free `max_pending`
  slots, so a backlog drains instead of deadlocking. Each step is published once.
- Cowork may still have acted when a result is missing, the task is abandoned or
  cancelled, or approval was refused. Treat effects as unknown and check Cowork's
  run history, not the fs status.

**Loop prevention.** The bridge posts only for coordinator assignments addressed
to its participant, at most once per task, and never in response to Teams content.
It accepts only replies from the separate responder account, so its own AMP_TASK
posts are never parsed as results. It writes results only as status messages, so
they cannot create assignments. On the Cowork side, the mention-only trigger,
the instructions and Cowork's own loop protection prevent re-triggering.

**Bounds.**

- Per round: feed pages up to HEAD (at most 100), one `/me` check per process, `scan_pages` + `max_outstanding` × `reply_pages` GETs, and at most one POST.
- At most `max_outstanding` dispatched or ambiguous tasks.
- HTTP bodies ≤ 4 MiB, message HTML ≤ 256 KiB, ≤ 20 forwarded links, and text truncated to fit `max_message_bytes`.
- Bridge rows are bounded by the namespace's `max_events`.
- Run only one bridge per participant. The state lock is local, not a distributed fence.

## What is tested, and what is not

Local tests (`tests/test_copilot365.py`) cover:

- A fake Graph that implements the documented request and response shapes, including an @mention contract check.
- A real loopback HTTP server exercising urllib, the device-code flow, refresh-token rotation, no-redirect, paging and size bounds.
- A spawned bridge killed after the Teams commit, which reconciles without reposting.
- Simulated draft-and-approve latency, timeout → `blocked` → late result.
- Forged, misthreaded, deleted, wrong-sender, wrong-channel and duplicate results.
- Prose headers, earliest-wins across roots and replies, long threads swept across rounds in either order, per-thread read failures, feed-backlog cancellation before dispatch, client outbox backlog, and write-side TLS errors.
- Policy and link rejection, cancellation, capacity, throttling, secret permissions, leak scans, fixed identity and state binding, CLI, and fs-status crash windows.

**No live Microsoft tenant, Teams, Graph, Entra or Cowork was used.** Before
production, run a tenant validation:

- Login and `check --online`.
- One allowlisted assignment, with the trigger firing, Cowork's approval prompt, the reply's `from.user.id` and threading, and the end-to-end latency.
- A refused approval.
- A Cowork rate-limit case.
- Token revocation and re-login.
- Bridge restart during a post.
- Conditional Access behavior.

Until then the Cowork side is a documented, unproven assumption.
