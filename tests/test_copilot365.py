"""Mocked Microsoft Graph contract tests for the copilot365 bridge.

The fake implements only documented request/response shapes used by the bridge.
It is not Microsoft Teams or Copilot Cowork; live tenant behavior is untested.
"""
from __future__ import annotations

import html
import http.server
import json
import multiprocessing
import os
import re
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from fs_exec.channel import (
    Broker,
    ChannelConfig,
    Client,
    Participant,
    provision,
    read_events,
)
from fs_exec.copilot365 import (
    GRAPH,
    LOGIN,
    Bridge,
    BridgeConfig,
    Graph,
    GraphError,
    NotSent,
    Tokens,
    device_login,
    read_secret,
    teams_text,
)
from fs_exec.copilot365 import http as graph_http

TENANT = "11111111-1111-1111-1111-111111111111"
CLIENT = "22222222-2222-2222-2222-222222222222"
TEAM = "33333333-3333-3333-3333-333333333333"
CHANNEL = "19:ampcopilot0123@thread.tacv2"
BRIDGE_USER = "44444444-4444-4444-4444-444444444444"
COWORK_USER = "55555555-5555-5555-5555-555555555555"
OTHER_USER = "66666666-6666-6666-6666-666666666666"
DOCS = "contoso.sharepoint.com"
ONEDRIVE = "contoso-my.sharepoint.com"
SECONDS = 1_000_000_000


class FakeGraph:
    """Documented Graph shapes: chatMessage POST/list/replies, /me, OAuth token."""

    def __init__(self, graph=GRAPH, login=LOGIN):
        self.graph, self.login = graph, login
        self.messages: dict[str, dict] = {}
        self.next_id = 1700000000000
        self.refresh = "rt-initial-0000000000"
        self.tokens = {}
        self.issued = 0
        self.log = []
        self.post_faults = []
        self.me_override = None
        self.reply_order = "newest"
        self.reply_faults = {}
        self.lock = threading.Lock()

    # Helpers used by the simulated Cowork/human side.
    def add(self, user, content, reply_to=None, content_type="html", **extra):
        with self.lock:
            self.next_id += 1
            mid = str(self.next_id)
            message = {"id": mid, "replyToId": reply_to, "messageType": "message", "deletedDateTime": None,
                       "createdDateTime": (datetime(2026, 11, 2, tzinfo=UTC) + timedelta(milliseconds=self.next_id - 1700000000000)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                       "lastModifiedDateTime": None, "from": {"application": None, "device": None,
                                                              "user": {"id": user, "displayName": "x", "userIdentityType": "aadUser"}},
                       "body": {"contentType": content_type, "content": content},
                       "channelIdentity": {"teamId": TEAM, "channelId": CHANNEL}, "attachments": [], "mentions": [],
                       "webUrl": f"https://teams.microsoft.com/l/message/{urllib.parse.quote(CHANNEL)}/{mid}"}
            message.update(extra)
            self.messages[mid] = message
            return message

    def roots(self, user=None):
        return [m for m in self.messages.values() if m["replyToId"] is None and (user is None or m["from"]["user"]["id"] == user)]

    def replies(self, mid):
        return [m for m in self.messages.values() if m["replyToId"] == mid]

    def __call__(self, method, url, headers, body, timeout=30, limit=4 * 1024 * 1024):
        self.log.append((method, url, dict(headers), body))
        parsed = urllib.parse.urlsplit(url)
        if url.startswith(self.login + "/"):
            return self.oauth(url[len(self.login):].split("?")[0], urllib.parse.parse_qs(body.decode()))
        if not url.startswith(self.graph + "/"):
            raise AssertionError(f"unexpected host {url}")
        auth = headers.get("Authorization", "")
        user = self.tokens.get(auth.removeprefix("Bearer "))
        if not auth.startswith("Bearer ") or user is None:
            return self.error(401, "InvalidAuthenticationToken")
        path = url[len(self.graph):].split("?")[0]
        query = urllib.parse.parse_qs(parsed.query)
        if path == "/me":
            return self.ok({"id": self.me_override or user})
        prefix = f"/teams/{TEAM}/channels/{CHANNEL}/messages"
        if not path.startswith(prefix):
            return self.error(404, "NotFound")
        rest = path[len(prefix):]
        if method == "POST" and rest == "":
            return self.post(json.loads(body), user)
        if method == "GET" and rest == "":
            # Documented: sorted by last-modified date of the entire reply chain.
            chain = lambda m: max([int(m["id"]), *(int(r["id"]) for r in self.replies(m["id"]))])
            return self.page(sorted(self.roots(), key=chain, reverse=True), query, url)
        match = re.fullmatch(r"/(\d+)(/replies)?", rest)
        if method == "GET" and match and match.group(1) in self.messages:
            if match.group(2):
                if match.group(1) in self.reply_faults:
                    return self.error(self.reply_faults[match.group(1)], "NotFound")
                replies = sorted(self.replies(match.group(1)), key=lambda m: int(m["id"]), reverse=self.reply_order == "newest")
                return self.page(replies, query, url)
            return self.ok(self.messages[match.group(1)])
        return self.error(404, "NotFound")

    def page(self, items, query, url):
        top = int(query.get("$top", ["20"])[0])
        assert top <= 50, "Graph documents a maximum $top of 50"
        skip = int(query.get("$skiptoken", ["0"])[0])
        page = {"value": items[skip:skip + top]}
        if skip + top < len(items):
            base = url.split("?")[0]
            page["@odata.nextLink"] = f"{base}?$top={top}&$skiptoken={skip + top}"
        return self.ok(page)

    def post(self, payload, user):
        # Contract from "Send chatMessage in a channel" example 2 (@mention).
        assert set(payload) == {"body", "mentions"}
        assert payload["body"]["contentType"] == "html"
        mention = payload["mentions"][0]
        assert mention["id"] == 0 and mention["mentioned"]["user"]["userIdentityType"] == "aadUser"
        assert f'<at id="0">{html.escape(mention["mentionText"])}</at>' in payload["body"]["content"]
        fault = self.post_faults.pop(0) if self.post_faults else None
        if fault == "refused":
            raise NotSent("ConnectionRefusedError")
        if isinstance(fault, int):
            return self.error(fault, "Throttled" if fault == 429 else "ServiceError", {"Retry-After": "120"} if fault == 429 else {})
        message = self.add(user, payload["body"]["content"], mentions=payload["mentions"])
        if fault == "lost-response":
            raise TimeoutError("response lost after server commit")
        if fault == "created-503":
            return self.error(503, "ServiceUnavailable")
        return 201, {"Content-Type": "application/json"}, json.dumps(message).encode()

    def oauth(self, path, form):
        assert path.startswith(f"/{TENANT}/oauth2/v2.0/")
        assert form["client_id"] == [CLIENT]
        if path.endswith("/devicecode"):
            return self.ok({"device_code": "device-1", "user_code": "ABCD", "verification_uri": "https://microsoft.com/devicelogin",
                            "expires_in": 900, "interval": 1, "message": "To sign in, enter ABCD"})
        grant = form["grant_type"][0]
        if grant == "urn:ietf:params:oauth:grant-type:device_code":
            if form["device_code"] != ["device-1"]:
                return self.error(400, "x", body={"error": "expired_token"})
            if not getattr(self, "device_ready", False):
                self.device_ready = True
                return 400, {}, json.dumps({"error": "authorization_pending"}).encode()
        elif grant != "refresh_token" or form["refresh_token"] != [self.refresh] or "offline_access" not in form["scope"][0]:
            return 400, {}, json.dumps({"error": "invalid_grant", "error_description": "secret detail AADSTS"}).encode()
        self.issued += 1
        access = f"access-token-{self.issued:04d}-secret"
        self.tokens[access] = getattr(self, "login_user", BRIDGE_USER)
        self.refresh = f"rt-rotated-{self.issued:04d}-secret"
        return self.ok({"access_token": access, "expires_in": 3600, "refresh_token": self.refresh, "token_type": "Bearer"})

    @staticmethod
    def ok(value):
        return 200, {"Content-Type": "application/json"}, json.dumps(value).encode()

    @staticmethod
    def error(status, code, headers=None, body=None):
        return status, headers or {}, json.dumps(body or {"error": {"code": code, "message": "detail"}}).encode()

    def posts(self):
        return [entry for entry in self.log if entry[0] == "POST" and entry[1].startswith(self.graph)]


class Cowork:
    """Simulated event-driven task: mention trigger, draft, human approval."""

    def __init__(self, fake):
        self.fake, self.seen, self.drafts = fake, set(), []

    def fire(self):
        for message in self.fake.roots():
            mentioned = any(m["mentioned"]["user"]["id"] == COWORK_USER for m in message.get("mentions", []))
            if message["id"] in self.seen or not mentioned:
                continue
            self.seen.add(message["id"])
            text, _, _ = teams_text(message["body"])
            task = re.search(r"^task: (\S+)$", text, re.MULTILINE).group(1)
            dispatch = re.search(r"^dispatch: (\S+)$", text, re.MULTILINE).group(1)
            self.drafts.append((message["id"], task, dispatch))

    def approve(self, status="completed", summary="Done.", extra=""):
        mid, task, dispatch = self.drafts.pop(0)
        content = f"<p>AMP_RESULT {task} {dispatch} {status}</p><p>{summary}</p>{extra}"
        return self.fake.add(COWORK_USER, content, reply_to=mid)


def crash_after_post_worker(config, base):
    from fs_exec import copilot365

    original = copilot365.Graph.post_message

    def post_then_die(self, content):
        original(self, content)
        os._exit(83)

    def deny(event, args):
        if event in {"subprocess.Popen", "os.system", "os.exec", "os.spawn", "os.fork", "os.posix_spawn"}:
            raise AssertionError("bridge attempted process execution")

    sys.addaudithook(deny)
    graph = copilot365.Graph(config, copilot365.Tokens(config, copilot365.http, base + "/login"), copilot365.http, base + "/v1.0")
    with patch.object(copilot365.Graph, "post_message", post_then_die), copilot365.Bridge(config, graph) as bridge:
        bridge.once()


class Server:
    """Real loopback HTTP server so urllib, headers and status codes are exercised."""

    def __init__(self, fake):
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def handle_one(self, method):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))) or None
                url = outer.base + self.path
                if "/redirect" in self.path:
                    self.send_response(302)
                    self.send_header("Location", outer.base + "/v1.0/me")
                    self.end_headers()
                    return
                try:
                    status, headers, data = fake(method, url, dict(self.headers), body)
                except (NotSent, TimeoutError):
                    self.close_connection = True
                    return
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                self.handle_one("GET")

            def do_POST(self):
                self.handle_one("POST")

            def log_message(self, *args):
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        fake.graph, fake.login = self.base + "/v1.0", self.base + "/login"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class Copilot365Tests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        participants = (Participant("amp", self.base / "mail/amp", "coordinator"),
                        Participant("copilot", self.base / "mail/copilot"),
                        Participant("claude", self.base / "mail/claude"))
        self.channel = ChannelConfig("team-one", self.base / "broker", participants)
        provision(self.channel)
        self.channel_path = self.base / "channel.toml"
        rows = [f'[channel]\nchannel = "team-one"\nstate_dir = "{self.channel.state_dir}"\n']
        rows += [f'[[participants]]\nname = "{p.name}"\nrole = "{p.role}"\nroot = "{p.root}"\n' for p in participants]
        self.channel_path.write_text("\n".join(rows))
        self.secret = self.base / "secrets/bridge.json"
        self.secret.parent.mkdir(mode=0o700)
        self.fake = FakeGraph()
        self.write_secret(self.fake.refresh)
        self.config = BridgeConfig(self.channel_path, "copilot", self.base / "bridge-state", self.secret, TENANT, CLIENT, TEAM,
                                   CHANNEL, BRIDGE_USER, COWORK_USER, "Cowork Owner", ("c365-",), (DOCS, ONEDRIVE),
                                   max_outstanding=2, result_timeout=600)
        self.now = 1_800_000_000 * SECONDS
        self.cowork = Cowork(self.fake)
        self.keys = 0

    def write_secret(self, token):
        self.secret.write_text(json.dumps({"refresh_token": token}))
        self.secret.chmod(0o600)

    def bridge(self, config=None, fake=None):
        config = config or self.config
        fake = fake or self.fake
        return Bridge(config, Graph(config, Tokens(config, fake, fake.login), fake, fake.graph), clock=lambda: self.now)

    def send(self, author, message):
        self.keys += 1
        with Client(self.channel, author, self.base / f"{author}-private") as client:
            seq = client.queue(message, f"k{self.keys}")
            client.sync()
        return seq

    def assign(self, task, text="Summarize the plan.", assignee="copilot"):
        self.send("amp", {"kind": "assign", "task": task, "assignee": assignee, "text": text})
        self.broker()

    def broker(self):
        with Broker(self.channel) as broker:
            while broker.once()["processed"]:
                pass

    def cycle(self, bridge, rounds=4):
        result = None
        for _ in range(rounds):
            self.broker()
            result = bridge.once()
            self.broker()
        return result

    def events(self, task=None):
        events = [r["event"] for r in read_events(self.channel, "amp", 0, 100)]
        return [e for e in events if task is None or (e["message"] or {}).get("task") == task]

    def statuses(self, task):
        return [(e["author"], e["message"]["status"]) for e in self.events(task) if e["accepted"] and e["message"]["kind"] == "status"]

    def test_assignment_to_teams_and_approved_result_back_into_channel(self):
        link = f"https://{DOCS}/sites/plan/Shared%20Documents/plan.docx"
        self.assign("c365-plan", f"Summarize <plan> & risks.\nSource: {link}")
        with self.bridge() as bridge:
            self.cycle(bridge)
            [post] = self.fake.posts()
            self.assertEqual(post[1], f"{GRAPH}/teams/{TEAM}/channels/{CHANNEL}/messages")
            content = json.loads(post[3])["body"]["content"]
            self.assertIn("Summarize &lt;plan&gt; &amp; risks.<br>Source: " + link, content)
            self.assertIn("task: c365-plan<br>dispatch: ", content)
            [root] = self.fake.roots(BRIDGE_USER)
            self.assertEqual(self.statuses("c365-plan"), [("copilot", "running")])
            self.assertIn(root["id"], self.events("c365-plan")[-1]["message"]["text"])

            # Draft-and-approve default: no reply until the owner approves.
            self.cowork.fire()
            self.now += 601 * SECONDS
            self.cycle(bridge)
            self.assertEqual(self.statuses("c365-plan"), [("copilot", "running"), ("copilot", "blocked")])
            self.assertIn("approval", self.events("c365-plan")[-1]["message"]["text"])

            extra = (f'<p><a href="https://{ONEDRIVE}/personal/o/Documents/summary.docx">summary</a> '
                     'and https://evil.example/x and <a href="file:///C:/Users/o/OneDrive/x">local</a></p>')
            reply = self.cowork.approve(summary="Plan &amp; risks summarized.", extra=extra)
            reply["attachments"] = [{"id": "a", "contentType": "reference", "name": "notes.docx",
                                     "contentUrl": f"https://{DOCS}/sites/plan/Shared%20Documents/notes.docx"}]
            self.cycle(bridge)
            self.assertEqual(self.statuses("c365-plan"), [("copilot", "running"), ("copilot", "blocked"),
                                                         ("copilot", "running"), ("copilot", "completed")])
            text = self.events("c365-plan")[-1]["message"]["text"]
            self.assertIn(f"Teams message {reply['id']}", text)
            self.assertIn("Plan & risks summarized.", text)
            self.assertIn(f"- https://{ONEDRIVE}/personal/o/Documents/summary.docx", text)
            self.assertIn(f"- https://{DOCS}/sites/plan/Shared%20Documents/notes.docx", text)
            self.assertNotIn("evil.example", text)
            self.assertNotIn("file://", text)
            self.assertIn("2 non-allowlisted link(s) withheld", text)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "done")
            self.cycle(bridge)
        self.assertEqual(len(self.fake.posts()), 1)
        self.assertEqual(len(self.statuses("c365-plan")), 4)

    def test_policy_rejections_never_reach_teams(self):
        self.assign("other-task")
        self.assign("c365-link", "Read http://contoso.sharepoint.com/insecure")
        self.assign("c365-foreign", "Read https://example.com/doc")
        self.assign("c365-big", "x" * 8001)
        self.assign("c365-claude", "Not ours", assignee="claude")
        with self.bridge() as bridge:
            self.cycle(bridge, 6)
            phases = {t["task"]: (t["phase"], t["fs_status"]) for t in bridge.status()["tasks"]}
        self.assertEqual(self.fake.posts(), [])
        self.assertEqual(set(phases), {"other-task", "c365-link", "c365-foreign", "c365-big"})
        for task, phase in phases.items():
            self.assertEqual(phase, ("done", "failed"))
            self.assertEqual(self.statuses(task), [("copilot", "running"), ("copilot", "failed")])
        self.assertIn("allowlist", self.events("other-task")[-1]["message"]["text"])
        self.assertEqual(self.statuses("c365-claude"), [])

    def test_only_fixed_responder_threaded_matching_result_is_accepted_once(self):
        self.assign("c365-one")
        self.assign("c365-two")
        with self.bridge() as bridge:
            self.cycle(bridge, 3)
            roots = {json.loads(p[3])["body"]["content"].split("task: ")[1].split("<")[0]: p for p in self.fake.posts()}
            self.assertEqual(set(roots), {"c365-one", "c365-two"})
            self.cowork.fire()
            (one_id, _, one_dispatch), (two_id, _, two_dispatch) = sorted(self.cowork.drafts, key=lambda d: d[1])
            forged = [
                (BRIDGE_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", one_id, {}),
                (OTHER_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", one_id, {}),
                (COWORK_USER, f"<p>AMP_RESULT c365-one {'0' * 32} completed</p>", one_id, {}),
                (COWORK_USER, f"<p>AMP_RESULT c365-two {two_dispatch} completed</p>", one_id, {}),
                (COWORK_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", two_id, {}),
                (COWORK_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", one_id, {"deletedDateTime": "2026-11-02T00:00:00Z"}),
                (COWORK_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", one_id, {"messageType": "systemEventMessage"}),
                (COWORK_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", one_id, {"channelIdentity": {"teamId": TEAM, "channelId": "19:other@thread.tacv2"}}),
                (COWORK_USER, f"<p>Note</p><p>AMP_RESULT c365-one {one_dispatch} completed</p>", one_id, {}),
                (COWORK_USER, f"<p>AMP_RESULT c365-one {one_dispatch} done</p>", one_id, {}),
                (COWORK_USER, f"<p>AMP_RESULT c365-unknown {one_dispatch} completed</p>", None, {}),
                (COWORK_USER, f"<p>AMP_TASK v1</p><p>task: c365-three<br>dispatch: {'1' * 32}</p>", None,
                 {"mentions": [{"mentioned": {"user": {"id": COWORK_USER}}}]}),
            ]
            for user, content, reply_to, extra in forged:
                self.fake.add(user, content, reply_to=reply_to, **extra)
            self.cycle(bridge, 3)
            self.assertEqual(self.statuses("c365-one"), [("copilot", "running")])
            self.assertEqual(self.statuses("c365-two"), [("copilot", "running")])
            self.assertEqual(self.statuses("c365-three"), [])
            # Leading @mention text is tolerated; first valid result wins.
            first = self.fake.add(COWORK_USER, f'<p><at id="0">Amp Bridge</at> AMP_RESULT c365-one {one_dispatch} failed</p><p>Could not open file.</p>', reply_to=one_id)
            self.fake.add(COWORK_USER, f"<p>AMP_RESULT c365-one {one_dispatch} completed</p>", reply_to=one_id)
            # A root-level reply (not threaded) is accepted for task two.
            self.fake.add(COWORK_USER, f"AMP_RESULT c365-two {two_dispatch} completed\nAll good.", content_type="text")
            self.cycle(bridge, 3)
            self.cycle(bridge, 2)
        self.assertEqual(self.statuses("c365-one"), [("copilot", "running"), ("copilot", "failed")])
        self.assertIn(first["id"], self.events("c365-one")[-1]["message"]["text"])
        self.assertEqual(self.statuses("c365-two"), [("copilot", "running"), ("copilot", "completed")])
        self.assertEqual(len(self.fake.posts()), 2)

    def test_ambiguous_post_outcomes_reconcile_without_automatic_repost(self):
        self.fake.post_faults = ["lost-response", "created-503", 503, "refused", None, 400]
        for task in ("c365-lost", "c365-503c", "c365-503", "c365-refused", "c365-bad"):
            self.assign(task)
        config = replace(self.config, max_outstanding=8)
        with self.bridge(config) as bridge:
            for _ in range(8):
                self.cycle(bridge, 1)
            tasks = {t["task"]: t for t in bridge.status()["tasks"]}
            # Server committed before the error: found by dispatch ID, not reposted.
            self.assertEqual(tasks["c365-lost"]["phase"], "posted")
            self.assertEqual(tasks["c365-503c"]["phase"], "posted")
            # Not created, but the 5xx is ambiguous: wait for an operator decision.
            self.assertEqual(tasks["c365-503"]["phase"], "ambiguous")
            self.assertEqual(self.statuses("c365-503"), [("copilot", "blocked")])
            # Refused connection is known unsent and retried exactly once more.
            self.assertEqual(tasks["c365-refused"]["phase"], "posted")
            self.assertEqual((tasks["c365-bad"]["phase"], tasks["c365-bad"]["fs_status"]), ("done", "failed"))
            bodies = [json.loads(p[3])["body"]["content"] for p in self.fake.posts()]
            for task in ("c365-lost", "c365-503c", "c365-503"):
                self.assertEqual(sum(f"task: {task}<" in b for b in bodies), 1, task)
            self.assertEqual(len(self.fake.roots(BRIDGE_USER)), 3)
            with self.assertRaises(ValueError):
                bridge.resolve("c365-503", adopt=tasks["c365-lost"]["message_id"])
            bridge.resolve("c365-503", repost=True)
            self.cycle(bridge, 3)
            self.assertEqual(bridge.status()["tasks"][2]["phase"], "posted")
            self.assertEqual(self.statuses("c365-503"), [("copilot", "blocked"), ("copilot", "running")])
            bridge.resolve("c365-refused", abandon=True)
            self.cycle(bridge, 3)
        self.assertEqual(self.statuses("c365-refused"), [("copilot", "running"), ("copilot", "failed")])
        self.assertIn("abandoned", self.events("c365-refused")[-1]["message"]["text"])

    def test_unresolved_post_is_reported_blocked_when_reconciliation_is_unavailable(self):
        self.assign("c365-ambiguous")
        self.fake.post_faults = ["created-503"]
        with self.bridge() as bridge:
            self.cycle(bridge, 1)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "ambiguous")
        self.now += 700 * SECONDS
        for _ in range(3):  # Restarts while channel reads are forbidden.
            with self.bridge() as bridge, patch.object(Graph, "list_roots", side_effect=GraphError(403, "Forbidden")):
                self.cycle(bridge, 2)
                row = bridge.status()["tasks"][0]
                self.assertEqual((row["phase"], row["fs_status"]), ("ambiguous", "blocked"))
                self.assertIn("POST outcome unknown", row["note"])
        self.assertEqual(len(self.fake.roots(BRIDGE_USER)), 1)
        self.assertEqual(self.statuses("c365-ambiguous"), [("copilot", "blocked")])
        with self.bridge() as bridge:  # Reads recover: adopt, never repost.
            self.cycle(bridge, 2)
        self.assertEqual(len(self.fake.posts()), 1)
        self.assertEqual(self.statuses("c365-ambiguous"), [("copilot", "blocked"), ("copilot", "running")])
        # A crash during POST leaves a durable posting intent: same reporting.
        self.assign("c365-crashed")
        with self.bridge() as bridge, patch.object(Graph, "post_message", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.cycle(bridge, 1)
        with self.bridge() as bridge, patch.object(Graph, "list_roots", side_effect=GraphError(403, "Forbidden")):
            self.cycle(bridge, 2)
            self.assertEqual(bridge.status()["tasks"][1]["phase"], "posting")
        self.assertEqual(self.statuses("c365-crashed"), [("copilot", "blocked")])
        self.assertEqual(len(self.fake.posts()), 1)

    def test_root_scan_outage_defers_result_selection(self):
        self.assign("c365-defer")
        with self.bridge() as bridge:
            self.cycle(bridge, 2)
            self.cowork.fire()
            mid, task, dispatch = self.cowork.drafts[0]
            early = self.fake.add(COWORK_USER, f"<p>AMP_RESULT {task} {dispatch} failed</p>")  # Earlier root.
            self.fake.add(COWORK_USER, f"<p>AMP_RESULT {task} {dispatch} completed</p>", reply_to=mid)
            with patch.object(Graph, "list_roots", side_effect=GraphError(503, "ServiceUnavailable")):
                self.cycle(bridge, 2)
                self.assertIsNone(bridge.status()["tasks"][0]["result_id"])
            self.cycle(bridge, 3)
            self.assertEqual(bridge.status()["tasks"][0]["result_id"], early["id"])
        self.assertEqual(self.statuses("c365-defer"), [("copilot", "running"), ("copilot", "failed")])

    def test_crash_before_post_is_ambiguous_and_operator_can_adopt(self):
        self.assign("c365-crash")
        with self.bridge() as bridge, patch.object(Graph, "post_message", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.cycle(bridge, 1)
        # The post actually happened elsewhere (for example, a previous process).
        with self.bridge() as bridge:
            dispatch = bridge.status()["tasks"][0]["dispatch"]
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "posting")
            self.cycle(bridge, 2)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "ambiguous")
            self.assertEqual(self.fake.posts(), [])
            forged = self.fake.add(OTHER_USER, f"<p>AMP_TASK v1</p><p>task: c365-crash<br>dispatch: {dispatch}</p>")
            with self.assertRaises(ValueError):
                bridge.resolve("c365-crash", adopt=forged["id"])
            # Older than the bounded scan window, so the operator adopts it.
            real = self.fake.add(BRIDGE_USER, f"<p>AMP_TASK v1</p><p>task: c365-crash<br>dispatch: {dispatch}</p>")
            for index in range(120):
                self.fake.add(OTHER_USER, f"<p>chatter {index}</p>")
            self.cycle(bridge, 2)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "ambiguous")
            bridge.resolve("c365-crash", adopt=real["id"])
            self.cycle(bridge, 2)
        self.assertEqual(self.statuses("c365-crash"), [("copilot", "blocked"), ("copilot", "running")])
        self.assertEqual(self.fake.posts(), [])

    def test_abrupt_death_after_teams_commit_reconciles_from_real_http_server(self):
        server = Server(self.fake)
        self.addCleanup(server.close)
        self.assign("c365-death")
        context = multiprocessing.get_context("spawn")
        process = context.Process(target=crash_after_post_worker, args=(self.config, server.base))
        process.start()
        process.join(30)
        self.assertEqual(process.exitcode, 83)
        self.assertEqual(len(self.fake.posts()), 1)
        self.write_secret(self.fake.refresh)  # Child rotated it in its own secret file.
        with self.bridge() as bridge:
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "posting")
            self.cycle(bridge, 2)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "posted")
            self.cowork.fire()
            self.cowork.approve()
            self.cycle(bridge, 3)
        self.assertEqual(len(self.fake.posts()), 1)
        self.assertEqual(self.statuses("c365-death"), [("copilot", "running"), ("copilot", "completed")])

    def test_status_publication_crash_windows_keep_one_identity(self):
        self.assign("c365-status")
        from fs_exec import copilot365

        original = copilot365.Bridge.update

        def die_on_seq(self, task, **fields):
            if "pending_seq" in fields:
                raise KeyboardInterrupt
            original(self, task, **fields)

        with self.bridge() as bridge, patch.object(copilot365.Bridge, "update", die_on_seq), self.assertRaises(KeyboardInterrupt):
            bridge.once()
        # A concurrently running broker accepts the queued status before restart.
        with Client(self.channel, "copilot", self.config.state_dir / "channel") as client:
            self.assertEqual(client.sync(), [1])
        self.broker()
        with self.bridge() as bridge:
            row = bridge.status()["tasks"][0]
            self.assertIsNone(row["pending_seq"])
            self.cycle(bridge, 3)
            self.assertEqual(bridge.status()["pending_channel_sends"], [])
        with patch("fs_exec.copilot365.Client.queue", side_effect=KeyboardInterrupt), self.bridge() as bridge:
            self.cowork.fire()
            self.cowork.approve()
            with self.assertRaises(KeyboardInterrupt):
                bridge.once()
        with self.bridge() as bridge:
            self.cycle(bridge, 3)
        self.assertEqual(self.statuses("c365-status"), [("copilot", "running"), ("copilot", "completed")])
        authored = [e["author_seq"] for e in self.events() if e["author"] == "copilot"]
        self.assertEqual(authored, [1, 2])

    def test_cancel_and_bounded_outstanding_work(self):
        for index in range(5):
            self.assign(f"c365-t{index}")
        with self.bridge() as bridge:
            bridge.once()
            self.assertEqual(len(self.fake.posts()), 1)  # One external post per round.
            self.cycle(bridge, 4)
            self.assertEqual(len(self.fake.posts()), 2)  # max_outstanding.
            seq = {t["task"]: t["revision"] for t in bridge.status()["tasks"]}
            self.send("amp", {"kind": "status", "task": "c365-t2", "revision": seq["c365-t2"], "status": "cancelled", "text": "stop"})
            self.send("amp", {"kind": "status", "task": "c365-t0", "revision": seq["c365-t0"], "status": "cancelled", "text": "stop"})
            self.cowork.fire()
            self.cowork.approve()  # Late result for the cancelled task is ignored.
            self.cycle(bridge, 4)
            phases = {t["task"]: t["phase"] for t in bridge.status()["tasks"]}
        self.assertEqual(phases, {"c365-t0": "cancelled", "c365-t1": "posted", "c365-t2": "cancelled", "c365-t3": "posted", "c365-t4": "queued"})
        self.assertEqual(self.statuses("c365-t0"), [("copilot", "running"), ("amp", "cancelled")])
        self.assertEqual(self.statuses("c365-t2"), [("amp", "cancelled")])
        self.assertEqual(len(self.fake.posts()), 3)

    def test_throttling_requeues_and_backs_off_without_requests(self):
        self.assign("c365-throttle")
        self.fake.post_faults = [429]
        with self.bridge() as bridge:
            result = self.cycle(bridge, 1)
            self.assertEqual(result["errors"][0]["detail"], "Microsoft Graph HTTP 429 Throttled")
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "queued")
            requests = len(self.fake.log)
            self.cycle(bridge, 2)
            self.assertEqual(len(self.fake.log), requests)
            bridge.backoff = 0
            self.cycle(bridge, 2)
        self.assertEqual(len(self.fake.roots(BRIDGE_USER)), 1)
        self.assertEqual(self.statuses("c365-throttle"), [("copilot", "running")])

    def test_write_side_tls_error_is_ambiguous_and_never_reposted(self):
        from fs_exec import copilot365

        for reason, unsent in ((ssl.SSLError(1, "write failed"), False), (ConnectionResetError(), False),
                               (ssl.SSLCertVerificationError(1, "bad certificate"), True),
                               (socket.gaierror(-2, "unknown host"), True), (ConnectionRefusedError(), True)):
            with self.subTest(reason=type(reason).__name__), \
                    patch.object(copilot365._OPENER, "open", side_effect=urllib.error.URLError(reason)):
                with self.assertRaises(OSError) as caught:
                    graph_http("POST", "https://graph.microsoft.com/v1.0/x", {}, b"{}")
                self.assertEqual(isinstance(caught.exception, NotSent), unsent)
        fault = {"armed": True}

        def transport(method, url, headers, body, timeout=30, limit=4 * 1024 * 1024):
            if method == "POST" and url.endswith("/messages") and fault.pop("armed", False):
                def committed_then_tls_failure(request, timeout):
                    self.fake(method, url, headers, body)  # Microsoft committed the message.
                    raise urllib.error.URLError(ssl.SSLError(1, "write failed"))
                with patch.object(copilot365._OPENER, "open", committed_then_tls_failure):
                    return graph_http(method, url, headers, body, timeout, limit)
            return self.fake(method, url, headers, body)

        self.assign("c365-tls")
        bridge = Bridge(self.config, Graph(self.config, Tokens(self.config, transport, self.fake.login), transport), clock=lambda: self.now)
        with bridge:
            self.assertEqual(self.cycle(bridge, 1)["errors"][0]["error"], "URLError")
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "ambiguous")
            self.cycle(bridge, 3)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "posted")
        self.assertEqual(len(self.fake.roots(BRIDGE_USER)), 1)
        self.assertEqual(self.statuses("c365-tls"), [("copilot", "blocked"), ("copilot", "running")])

    def test_status_backlog_beyond_client_pending_limit_drains(self):
        self.assertEqual(self.channel.max_pending, 32)
        for index in range(33):
            self.send("amp", {"kind": "assign", "task": f"other-{index}", "assignee": "copilot", "text": "x"})
        self.broker()
        with self.bridge() as bridge:
            bridge.once()
        with self.bridge() as bridge:  # Restart with a full client outbox.
            self.cycle(bridge, 8)
            self.assertEqual(bridge.status()["pending_channel_sends"], [])
            self.assertEqual(set(bridge.phases()), {"done"})
        for index in range(33):
            self.assertEqual(self.statuses(f"other-{index}"), [("copilot", "running"), ("copilot", "failed")])

    def test_feed_backlog_cancellation_is_applied_before_dispatch(self):
        self.send("amp", {"kind": "assign", "task": "c365-cancel", "assignee": "copilot", "text": "work"})
        for index in range(99):
            self.send("amp", {"kind": "chat", "text": f"filler {index}", "mentions": [], "reply_to": None})
        self.send("amp", {"kind": "status", "task": "c365-cancel", "revision": 1, "status": "cancelled", "text": "stop"})
        self.broker()
        self.assertEqual(read_events(self.channel, "amp", 100)[0]["event"]["message"]["status"], "cancelled")
        with self.bridge() as bridge:
            bridge.once()
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "cancelled")
        self.assertEqual(self.fake.posts(), [])

    def test_unreadable_thread_does_not_starve_other_results(self):
        self.assign("c365-a")
        self.assign("c365-b")
        with self.bridge() as bridge:
            self.cycle(bridge, 3)
            self.cowork.fire()
            drafts = sorted(self.cowork.drafts, key=lambda d: d[1])
            self.fake.reply_faults[drafts[0][0]] = 404
            self.cowork.drafts = [drafts[1]]
            self.cowork.approve()
            result = self.cycle(bridge, 3)
            self.assertIn("replies for c365-a", result["errors"][0]["detail"])
            tasks = {t["task"]: t for t in bridge.status()["tasks"]}
            self.assertEqual(tasks["c365-a"]["phase"], "posted")
            self.assertIn("reply read failed", tasks["c365-a"]["note"])
            self.now += 601 * SECONDS
            self.cycle(bridge, 2)
        self.assertEqual(self.statuses("c365-b"), [("copilot", "running"), ("copilot", "completed")])
        self.assertEqual(self.statuses("c365-a"), [("copilot", "running"), ("copilot", "blocked")])
        self.assertIn("reply read failed", self.events("c365-a")[-1]["message"]["text"])

    def test_prose_header_rejected_and_earliest_result_wins_across_roots_and_replies(self):
        self.assign("c365-quote")
        with self.bridge() as bridge:
            self.cycle(bridge, 2)
            self.cowork.fire()
            mid, task, dispatch = self.cowork.drafts[0]
            for content, kind in ((f"<p>Do not send AMP_RESULT {task} {dispatch} completed</p>", "html"),
                                  (f"@Cowork AMP_RESULT {task} {dispatch} completed", "text"),
                                  (f"<p><b>AMP_RESULT</b> {task} {dispatch} completed please</p>", "html")):
                self.fake.add(COWORK_USER, content, reply_to=mid, content_type=kind)
            self.cycle(bridge, 2)
            self.assertEqual(bridge.status()["tasks"][0]["result_id"], None)
            early = self.fake.add(COWORK_USER, f"<p>AMP_RESULT {task} {dispatch} failed</p>", reply_to=mid)
            self.fake.add(COWORK_USER, f"<p>AMP_RESULT {task} {dispatch} completed</p>")  # Later root post.
            self.cycle(bridge, 3)
        self.assertEqual(self.statuses("c365-quote"), [("copilot", "running"), ("copilot", "failed")])
        self.assertIn(early["id"], self.events("c365-quote")[-1]["message"]["text"])

    def test_long_threads_are_swept_across_rounds_without_losing_results(self):
        for order in ("newest", "oldest"):
            with self.subTest(order=order):
                self.fake.reply_order = order
                task = f"c365-long-{order}"
                self.assign(task)
                with self.bridge() as bridge:
                    self.cycle(bridge, 2)
                    self.cowork.fire()
                    mid, _, dispatch = self.cowork.drafts.pop()
                    for index in range(201 if order == "oldest" else 0):
                        self.fake.add(OTHER_USER, f"<p>noise {index}</p>", reply_to=mid)
                    early = self.fake.add(COWORK_USER, f"<p>AMP_RESULT {task} {dispatch} failed</p>", reply_to=mid)
                    for index in range(200):
                        self.fake.add(OTHER_USER, f"<p>later {index}</p>", reply_to=mid)
                    self.fake.add(COWORK_USER, f"<p>AMP_RESULT {task} {dispatch} completed</p>", reply_to=mid)
                    for _ in range(4):
                        self.cycle(bridge, 1)
                    row = next(t for t in bridge.status()["tasks"] if t["task"] == task)
                self.assertEqual(row["result_id"], early["id"])
                self.assertEqual(self.statuses(task)[-1], ("copilot", "failed"))

    def test_secret_handling_rotation_and_definite_token_failures(self):
        self.assign("c365-secret")
        self.secret.chmod(0o644)
        with self.bridge() as bridge:
            result = self.cycle(bridge, 2)
            self.assertEqual(result["errors"][0]["error"], "TokenError")
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "queued")
            self.secret.chmod(0o600)
            self.write_secret("rt-revoked-0000000000")
            result = self.cycle(bridge, 1)
            self.assertIn("invalid_grant", result["errors"][0]["detail"])
            self.assertNotIn("AADSTS", json.dumps(result))
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "queued")
            self.write_secret(self.fake.refresh)
            self.cycle(bridge, 2)
            self.assertEqual(bridge.status()["tasks"][0]["phase"], "posted")
        self.assertEqual(read_secret(self.secret), {"refresh_token": self.fake.refresh})
        self.assertEqual(self.secret.stat().st_mode & 0o777, 0o600)
        self.assertEqual(sorted(p.name for p in self.secret.parent.iterdir()), ["bridge.json"])
        leaked = [b"access-token-", b"rt-rotated-", b"rt-initial-"]
        for path in [*self.base.rglob("*")]:
            if path.is_file() and path != self.secret:
                data = path.read_bytes()
                for token in leaked:
                    self.assertNotIn(token, data, path)

    def test_identity_and_configuration_are_fixed(self):
        self.fake.me_override = OTHER_USER
        self.assign("c365-who")
        with self.bridge() as bridge:
            result = self.cycle(bridge, 2)
            self.assertIn("bridge account", result["errors"][0]["detail"])
        self.assertEqual(self.fake.posts(), [])
        with self.assertRaisesRegex(ValueError, "state belongs"):
            self.bridge(replace(self.config, channel_id="19:another@thread.tacv2")).__enter__()
        bad = [
            {"responder_user_id": BRIDGE_USER},
            {"task_prefixes": ()},
            {"task_prefixes": ("C365",)},
            {"allowed_link_hosts": ("example.com",)},
            {"allowed_link_hosts": ("contoso.sharepoint.com.evil.example",)},
            {"state_dir": self.base / "OneDrive - Contoso/bridge"},
            {"secret_file": self.config.state_dir / "secret.json"},
            {"channel_id": "19:x@thread.tacv2/../../me"},
            {"team_id": "not-a-guid"},
            {"poll_seconds": 1},
        ]
        for change in bad:
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(self.config, **change)
        for change in ({"participant": "amp"}, {"state_dir": self.channel.participant("copilot").root / "state"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(self.config, **change).load_channel()
        synced = self.channel_path.read_text().replace(str(self.base / "mail/claude"), str(self.base / "OneDrive/claude"))
        self.channel_path.write_text(synced)
        with self.assertRaisesRegex(ValueError, "cloud-synced"):
            self.config.load_channel()

    def test_real_http_device_login_redirect_and_paging_bounds(self):
        server = Server(self.fake)
        self.addCleanup(server.close)
        self.secret.unlink()
        self.fake.login_user = OTHER_USER
        out = Path(os.devnull).open("w")  # noqa: SIM115 - closed by cleanup
        self.addCleanup(out.close)
        args = {"transport": graph_http, "login": self.fake.login, "base": self.fake.graph, "out": out, "sleep": lambda s: None}
        with self.assertRaisesRegex(OSError, "not the configured bridge_user_id"):
            device_login(self.config, **args)
        self.assertFalse(self.secret.exists())
        self.fake.login_user, self.fake.device_ready = BRIDGE_USER, False
        self.assertEqual(device_login(self.config, **args), BRIDGE_USER)
        self.assertEqual(self.secret.stat().st_mode & 0o777, 0o600)
        graph = Graph(self.config, Tokens(self.config, graph_http, self.fake.login), graph_http, self.fake.graph)
        self.assertEqual(graph.me(), BRIDGE_USER)
        with self.assertRaisesRegex(Exception, "302"):
            graph.request("GET", server.base + "/v1.0/../redirect")
        # The redirect is not followed, so its target never receives the bearer.
        self.assertEqual([url for _, url, _, _ in self.fake.log if url.endswith("/v1.0/me")], [])
        with self.assertRaisesRegex(ValueError, "non-Graph"):
            graph.request("GET", "http://127.0.0.1:1/v1.0/me")
        for index in range(130):
            self.fake.add(OTHER_USER, f"<p>{index}</p>")
        self.assertEqual(len(graph.list_roots(2)), 100)
        self.assertEqual(len(graph.list_roots(10)), 130)

    def test_teams_html_text_extraction(self):
        text, links, header = teams_text({"contentType": "html", "content":
                                  '<div><p><at id="0">Amp&nbsp;Bridge</at>&nbsp;AMP_RESULT a b completed</p>'
                                  '<p>Line&amp;one<br>two <a href="https://x.sharepoint.com/a">doc</a></p>'
                                  "<script>ignored()</script></div>"})
        self.assertEqual(text.split("\n")[0], "@Amp Bridge AMP_RESULT a b completed")
        self.assertEqual(header, "AMP_RESULT a b completed")
        self.assertIn("Line&one\ntwo doc", text)
        self.assertEqual(links, ["https://x.sharepoint.com/a"])
        for body in (None, {"contentType": "html"}, {"contentType": "markdown", "content": "x"}, {"contentType": "text", "content": "x" * 300000}):
            with self.assertRaises(ValueError):
                teams_text(body)

    def test_cli_offline_commands_and_errors(self):
        config_path = self.base / "copilot365.toml"
        config_path.write_text(f"""[copilot365]
channel_config = "{self.channel_path}"
participant = "copilot"
state_dir = "{self.config.state_dir}"
secret_file = "{self.base / 'secrets/missing.json'}"
tenant_id = "{TENANT}"
client_id = "{CLIENT}"
team_id = "{TEAM.upper()}"
channel_id = "{CHANNEL}"
bridge_user_id = "{BRIDGE_USER}"
responder_user_id = "{COWORK_USER}"
responder_display_name = "Cowork Owner"
task_prefixes = ["c365-"]
allowed_link_hosts = ["{DOCS}"]
""")
        def cli(*args, expected=0):
            result = subprocess.run([sys.executable, "-m", "fs_exec.copilot365", "--config", str(config_path), *args],
                                    capture_output=True, text=True, timeout=30, check=False)
            self.assertEqual(result.returncode, expected, result.stderr)
            return json.loads(result.stdout) if result.stdout.strip() else json.loads(result.stderr)

        self.assertEqual(cli("check")["secret"], "missing; run login")
        self.assertEqual(cli("status"), {"cursor": 0, "pending_channel_sends": [], "tasks": []})
        self.assign("c365-cli")
        with self.bridge() as bridge:
            bridge.ingest()
        self.assertEqual(cli("resolve", "c365-cli", "--abandon")["phase"], "rejected")
        self.assertIn("ambiguous", cli("resolve", "c365-cli", "--repost", expected=2)["detail"])
        with self.bridge() as bridge, self.assertRaises(OSError):
            Bridge(self.config)  # The private state lock excludes a second bridge.
        config_path.write_text(config_path.read_text().replace('["c365-"]', "[]"))
        self.assertEqual(cli("check", expected=2)["error"], "ValueError")


if __name__ == "__main__":
    unittest.main()
