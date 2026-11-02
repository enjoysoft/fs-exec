"""Owner-controlled bridge between fs-channel and a Microsoft Teams channel.

The bridge is the fs-channel participant assigned Copilot work. It posts each
allowlisted assignment as one AMP_TASK root message, using delegated Microsoft
Graph permissions of a dedicated bridge account, to one fixed Teams channel. A
Microsoft 365 Copilot Cowork event-driven task that the responder user created
handles the message and replies with AMP_RESULT. The bridge accepts only that
user's reply for a task it dispatched and records it as fs-channel task status.

It never executes task or result text. Cowork has no documented callable API;
the Teams channel is the only integration point and Cowork approvals are human.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import secrets
import socket
import sqlite3
import ssl
import stat
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path

from .channel import ChannelConfig, Client, Store, name, read_events
from .rally import Rejected, object_json
from .util import (
    canonical_json,
    check_path,
    fsync_directory,
    read_bounded,
    sha256_bytes,
)

GRAPH = "https://graph.microsoft.com/v1.0"
LOGIN = "https://login.microsoftonline.com"
SCOPES = "offline_access User.Read ChannelMessage.Send ChannelMessage.Read.All"
GUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
CHANNEL_ID = re.compile(r"19:[A-Za-z0-9_-]{1,128}@thread\.(?:tacv2|skype)")
MESSAGE_ID = re.compile(r"[0-9]{1,20}")
LINK_HOST = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.sharepoint\.(?:com|us|cn)")
URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]{1,15}://[^\s<>\"']+")
DISPATCH = re.compile(r"^dispatch: ([0-9a-f]{32})$", re.MULTILINE)
RESULT = re.compile(r"AMP_RESULT ([a-z][a-z0-9_-]{0,47}) ([0-9a-f]{32}) (completed|failed)")
MENTION_OPEN, MENTION_CLOSE = "\x02", "\x03"
TRANSITIONS = {"assigned": {"running", "blocked", "cancelled"},
               "running": {"blocked", "completed", "failed", "cancelled"},
               "blocked": {"running", "failed", "cancelled"}}
ACTIVE = ("posting", "ambiguous", "posted")
TIMEOUT = 30
MAX_RESPONSE = 4 * 1024 * 1024
MAX_BODY = 256 * 1024
MAX_LINKS = 20


def guid(value: str) -> str:
    if not isinstance(value, str) or not GUID.fullmatch(value.lower()):
        raise ValueError("expected Microsoft Entra/Teams GUID")
    return value.lower()


def cloud_synced(path: Path) -> bool:
    """Heuristic: refuse live namespaces inside common sync-client folders."""
    return any(part.lower().startswith("onedrive") or "sharepoint" in part.lower()
               or part.lower() in {"dropbox", "google drive", "icloud drive", "icloud~com~apple~clouddocs"}
               for part in path.parts)


def overlaps(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


@dataclass(frozen=True)
class BridgeConfig:
    channel_config: Path
    participant: str
    state_dir: Path
    secret_file: Path
    tenant_id: str
    client_id: str
    team_id: str
    channel_id: str
    bridge_user_id: str
    responder_user_id: str
    responder_display_name: str
    task_prefixes: tuple[str, ...]
    allowed_link_hosts: tuple[str, ...]
    max_outstanding: int = 4
    result_timeout: int = 14400
    poll_seconds: int = 30
    max_task_chars: int = 8000
    max_result_chars: int = 8000
    scan_pages: int = 2
    reply_pages: int = 4

    def __post_init__(self):
        name(self.participant)
        for field in ("tenant_id", "client_id", "team_id", "bridge_user_id", "responder_user_id"):
            object.__setattr__(self, field, guid(getattr(self, field)))
        if self.bridge_user_id == self.responder_user_id:
            raise ValueError("bridge and Cowork responder must be different accounts (loop prevention)")
        if not isinstance(self.channel_id, str) or not CHANNEL_ID.fullmatch(self.channel_id):
            raise ValueError("expected Teams channel ID 19:...@thread.tacv2")
        display = self.responder_display_name
        if not isinstance(display, str) or not display.strip() or len(display) > 256 or any(ord(c) < 32 for c in display):
            raise ValueError("invalid responder display name")
        if not isinstance(self.task_prefixes, tuple) or not self.task_prefixes:
            raise ValueError("task_prefixes allowlist is required (no wildcard)")
        for prefix in self.task_prefixes:
            if not isinstance(prefix, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,46}", prefix):
                raise ValueError("task prefixes must be lowercase identifier prefixes")
        if not isinstance(self.allowed_link_hosts, tuple):
            raise Rejected("allowed_link_hosts must be a list")
        for host in self.allowed_link_hosts:
            if not isinstance(host, str) or not LINK_HOST.fullmatch(host):
                raise ValueError("allowed link hosts must be exact SharePoint/OneDrive host names")
        for path in (self.channel_config, self.state_dir, self.secret_file):
            check_path(path)
            if not path.is_absolute() or Path(os.path.abspath(path)) != path:
                raise ValueError("absolute normalized paths required")
        if overlaps(self.state_dir, self.secret_file):
            raise ValueError("secret file must not live inside bridge state")
        for path in (self.state_dir, self.secret_file):
            if cloud_synced(path):
                raise ValueError("bridge state and secrets must not be in a cloud-synced folder")
        bounds = {"max_outstanding": (1, 64), "result_timeout": (60, 30 * 86400), "poll_seconds": (10, 3600),
                  "max_task_chars": (1, 20000), "max_result_chars": (1, 20000), "scan_pages": (1, 10), "reply_pages": (1, 20)}
        for field, (low, high) in bounds.items():
            value = getattr(self, field)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{field} must be an integer in {low}..{high}")

    @classmethod
    def load(cls, path: Path) -> BridgeConfig:
        with path.open("rb") as stream:
            raw = dict(tomllib.load(stream)["copilot365"])
        for field in ("channel_config", "state_dir", "secret_file"):
            raw[field] = Path(raw[field])
        for field in ("task_prefixes", "allowed_link_hosts"):
            if not isinstance(raw.get(field), list):
                raise Rejected(f"{field} must be a list")
            raw[field] = tuple(raw[field])
        return cls(**raw)

    def load_channel(self) -> ChannelConfig:
        channel = ChannelConfig.load(self.channel_config)
        if channel.participant(self.participant).role != "agent":
            raise ValueError("bridge participant must be an fs-channel agent, not the coordinator")
        namespaces = [channel.state_dir, *(p.root for p in channel.participants)]
        for path in namespaces:
            if cloud_synced(path):
                raise ValueError("live fs-channel namespaces must not be in a cloud-synced folder")
            for private in (self.state_dir, self.secret_file):
                if overlaps(path, private):
                    raise ValueError("bridge state/secret must be outside fs-channel mailboxes and broker state")
        return channel

    def binding(self, channel: str) -> bytes:
        # Fixed identities: changing tenant/team/channel/accounts needs fresh state.
        return canonical_json({"role": "copilot365", "channel": channel, "participant": self.participant,
                               **{k: getattr(self, k) for k in ("tenant_id", "client_id", "team_id", "channel_id", "bridge_user_id", "responder_user_id")}})


# ---------------------------------------------------------------- Microsoft APIs


class NotSent(OSError):
    """The request is known not to have reached Microsoft; retry is safe."""


class TokenError(NotSent):
    pass


class GraphError(Exception):
    # Codes where Graph documents or HTTP semantics guarantee no message creation.
    DEFINITE = frozenset({400, 401, 403, 404, 405, 409, 411, 413, 415, 422, 429})

    def __init__(self, status: int, code: str, retry_after: int | None = None):
        super().__init__(f"Microsoft Graph HTTP {status} {code}")
        self.status, self.code, self.retry_after = status, code, retry_after

    @property
    def definite(self) -> bool:
        return self.status in self.DEFINITE


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # Never forward a bearer token or form secret to a redirect.


_OPENER = urllib.request.build_opener(_NoRedirect)


def http(method: str, url: str, headers: dict, body: bytes | None, timeout: float = TIMEOUT, limit: int = MAX_RESPONSE):
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        response = _OPENER.open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc
    except urllib.error.URLError as exc:
        # urllib also wraps write-side errors here; generic TLS/reset errors are
        # ambiguous. Name resolution, refusal and certificate rejection happen
        # while connecting, before any request byte is sent.
        if isinstance(exc.reason, (socket.gaierror, ConnectionRefusedError, ssl.SSLCertVerificationError)):
            raise NotSent(type(exc.reason).__name__) from None
        raise
    with response:
        data = response.read(limit + 1)
        if len(data) > limit:
            raise ValueError("response exceeds byte limit")
        return response.getcode(), dict(response.headers or {}), data


def _code(data: bytes, field: str) -> str:
    try:
        value = object_json(data)
        value = value.get(field) if field != "graph" else (value.get("error") or {}).get("code")
    except (ValueError, AttributeError, RecursionError):
        value = None
    # Only short codes are surfaced; descriptions may contain tenant details.
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value) else "unknown"


def read_secret(path: Path) -> dict:
    check_path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("secret file must be a regular single-link file")
    if os.name != "nt" and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ValueError("secret file must be owned by the bridge account with mode 0600")
    secret = object_json(read_bounded(path, 65536))
    if set(secret) != {"refresh_token"} or not isinstance(secret["refresh_token"], str) or not 16 <= len(secret["refresh_token"]) <= 16384:
        raise ValueError("secret file must contain only a refresh_token")
    return secret


def write_secret(path: Path, secret: dict) -> None:
    check_path(path)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(canonical_json(secret))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    fsync_directory(path.parent)


class Tokens:
    """Delegated refresh-token flow for the dedicated bridge account."""

    def __init__(self, config: BridgeConfig, transport=http, login: str = LOGIN):
        self.config, self.transport, self.login = config, transport, login
        self.access, self.expires = None, 0.0

    def endpoint(self, leaf: str) -> str:
        return f"{self.login}/{self.config.tenant_id}/oauth2/v2.0/{leaf}"

    def form(self, leaf: str, fields: dict) -> tuple[int, bytes]:
        body = urllib.parse.urlencode({"client_id": self.config.client_id, **fields}).encode()
        status, _, data = self.transport("POST", self.endpoint(leaf), {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}, body)
        return status, data

    def invalidate(self) -> None:
        self.access = None

    def get(self) -> str:
        if self.access and time.monotonic() < self.expires:
            return self.access
        secret = read_secret(self.config.secret_file)
        try:
            status, data = self.form("token", {"grant_type": "refresh_token", "refresh_token": secret["refresh_token"], "scope": SCOPES})
        except NotSent:
            raise
        except OSError as exc:
            raise TokenError(f"token endpoint unavailable: {type(exc).__name__}") from None
        if status != 200:
            raise TokenError(f"token refresh failed: {_code(data, 'error')}")
        token = object_json(data)
        access, expires = token.get("access_token"), token.get("expires_in")
        if not isinstance(access, str) or not access or type(expires) is not int or not 60 <= expires <= 86400:
            raise TokenError("invalid token response")
        rotated = token.get("refresh_token")
        if isinstance(rotated, str) and rotated != secret["refresh_token"]:
            write_secret(self.config.secret_file, {"refresh_token": rotated})
        self.access, self.expires = access, time.monotonic() + expires - 300
        return access


class Graph:
    def __init__(self, config: BridgeConfig, tokens: Tokens | None = None, transport=http, base: str = GRAPH):
        self.config, self.transport, self.base = config, transport, base
        self.tokens = tokens or Tokens(config, transport)
        self.channel_path = (f"/teams/{config.team_id}/channels/"
                             f"{urllib.parse.quote(config.channel_id, safe=':@.')}/messages")

    def request(self, method: str, target: str, payload: dict | None = None, token: str | None = None) -> dict:
        url = target if target.startswith(self.base + "/") else self.base + target
        if not url.startswith(self.base + "/") or "\\" in url:
            raise ValueError("refusing non-Graph URL")
        body = None if payload is None else canonical_json(payload)
        for attempt in (0, 1):
            try:
                bearer = token or self.tokens.get()
            except NotSent:
                raise
            except (OSError, ValueError) as exc:
                # Nothing reached Graph; never let a local secret error look ambiguous.
                raise TokenError(f"token unavailable: {type(exc).__name__}: {str(exc)[:120]}") from None
            headers = {"Authorization": f"Bearer {bearer}", "Accept": "application/json"}
            if body is not None:
                headers["Content-Type"] = "application/json"
            status, response_headers, data = self.transport(method, url, headers, body)
            if status == 401 and attempt == 0 and token is None:
                self.tokens.invalidate()  # 401 is never processed; one fresh token retry.
                continue
            break
        if not 200 <= status < 300:
            retry = next((v for k, v in response_headers.items() if k.lower() == "retry-after"), None)
            retry = int(retry) if isinstance(retry, str) and retry.isdigit() else None
            raise GraphError(status, _code(data, "graph"), None if retry is None else min(max(retry, 1), 3600))
        return object_json(data) if data else {}

    def me(self, token: str | None = None) -> str:
        return guid(self.request("GET", "/me?$select=id", token=token).get("id"))

    def post_message(self, content: str) -> dict:
        cfg = self.config
        return self.request("POST", self.channel_path, {
            "body": {"contentType": "html", "content": content},
            "mentions": [{"id": 0, "mentionText": cfg.responder_display_name,
                          "mentioned": {"user": {"displayName": cfg.responder_display_name,
                                                 "id": cfg.responder_user_id, "userIdentityType": "aadUser"}}}]})

    def get_message(self, message_id: str) -> dict:
        return self.request("GET", f"{self.channel_path}/{message_id}")

    def page(self, target: str) -> tuple[list, str | None]:
        page = self.request("GET", target)
        values, link = page.get("value"), page.get("@odata.nextLink")
        if not isinstance(values, list) or not (link is None or (isinstance(link, str) and len(link) <= 4096)):
            raise Rejected("invalid Graph collection")
        return values, link

    def _pages(self, target: str, pages: int) -> list[dict]:
        result = []
        for _ in range(pages):
            values, target = self.page(target)
            result.extend(values)
            if target is None:
                break
        return result

    def list_roots(self, pages: int) -> list[dict]:
        return self._pages(f"{self.channel_path}?$top=50", pages)

    def replies_url(self, message_id: str) -> str:
        return f"{self.channel_path}/{message_id}/replies?$top=50"


def device_login(config: BridgeConfig, transport=http, login: str = LOGIN, base: str = GRAPH, out=sys.stderr, sleep=time.sleep) -> str:
    """Interactive device-code sign-in of the dedicated bridge account."""
    tokens = Tokens(config, transport, login)
    status, data = tokens.form("devicecode", {"scope": SCOPES})
    if status != 200:
        raise TokenError(f"device code request failed: {_code(data, 'error')}")
    grant = object_json(data)
    message, device = grant.get("message"), grant.get("device_code")
    interval, expires = grant.get("interval", 5), grant.get("expires_in", 900)
    if not isinstance(message, str) or not isinstance(device, str) or type(interval) is not int or type(expires) is not int:
        raise TokenError("invalid device code response")
    print(message[:1000], file=out, flush=True)
    deadline = time.monotonic() + min(expires, 3600)
    while time.monotonic() < deadline:
        sleep(min(max(interval, 1), 60))
        status, data = tokens.form("token", {"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": device})
        if status == 200:
            token = object_json(data)
            if not isinstance(token.get("refresh_token"), str) or not isinstance(token.get("access_token"), str):
                raise TokenError("token response lacks offline_access refresh token")
            account = Graph(config, tokens, transport, base).me(token["access_token"])
            if account != config.bridge_user_id:
                raise TokenError("signed-in account is not the configured bridge_user_id; nothing saved")
            write_secret(config.secret_file, {"refresh_token": token["refresh_token"]})
            return account
        code = _code(data, "error")
        if code == "slow_down":
            interval += 5
        elif code != "authorization_pending":
            raise TokenError(f"device code sign-in failed: {code}")
    raise TokenError("device code expired")


# ---------------------------------------------------------------- Teams text


class _Text(HTMLParser):
    BLOCK = frozenset({"p", "div", "br", "li", "tr", "pre", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6"})

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.links, self.hidden = [], [], 0

    def handle_starttag(self, tag, attrs):
        if tag == "at":
            self.parts.append(MENTION_OPEN)
        if tag in {"script", "style"}:
            self.hidden += 1
        if tag in self.BLOCK:
            self.parts.append("\n")
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

    def handle_endtag(self, tag):
        if tag == "at":
            self.parts.append(MENTION_CLOSE)
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)
        if tag in self.BLOCK:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data.replace(MENTION_OPEN, "").replace(MENTION_CLOSE, ""))


def teams_text(body) -> tuple[str, list[str], str]:
    """Text, link targets and the first line without leading Teams @mentions."""
    if not isinstance(body, dict) or not isinstance(body.get("content"), str) or body.get("contentType") not in {"html", "text"}:
        raise Rejected("invalid message body")
    content = body["content"]
    if len(content) > MAX_BODY:
        raise Rejected("message body exceeds limit")
    links: list[str] = []
    content = content.replace(MENTION_OPEN, "").replace(MENTION_CLOSE, "")
    if body["contentType"] == "html":
        parser = _Text()
        parser.feed(content)
        parser.close()
        content, links = "".join(parser.parts), parser.links
    lines = (" ".join(line.replace("\xa0", " ").split()) for line in content.replace("\r", "\n").split("\n"))
    marked = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    # Only real <at> mention markup may precede a protocol header, never prose.
    header = re.sub(f"^(?:{MENTION_OPEN}[^{MENTION_OPEN}{MENTION_CLOSE}]*{MENTION_CLOSE} ?)+", "", marked.split("\n")[0]).strip()
    text = marked.replace(MENTION_OPEN, "@").replace(MENTION_CLOSE, "")
    return text, links + [u.rstrip(".,;:)]}") for u in URL.findall(text)], header


def link_allowed(url: str, hosts: tuple[str, ...]) -> bool:
    try:
        parts = urllib.parse.urlsplit(url)
        port = parts.port
    except ValueError:
        return False
    return parts.scheme == "https" and parts.hostname in hosts and port in (None, 443) and parts.username is None and parts.password is None


def teams_url(message: dict) -> str:
    url = message.get("webUrl")
    return url if isinstance(url, str) and len(url) < 2048 and url.startswith("https://teams.microsoft.com/l/message/") else ""


# ---------------------------------------------------------------- bridge


def timestamp(value) -> str:
    """Fixed-width UTC form so Graph timestamps of any precision order correctly."""
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) and len(value) <= 40 else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.tzinfo is None:
        return "~"  # Sorts after every valid timestamp.
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


def earliest(left: dict | None, right: dict | None) -> dict | None:
    if left is None or right is None:
        return left or right
    return min(left, right, key=lambda c: (c["created"], int(c["id"])))


SCHEMA = """
CREATE TABLE IF NOT EXISTS bridge_tasks (
    task TEXT PRIMARY KEY, assign_seq INTEGER NOT NULL, text TEXT NOT NULL,
    phase TEXT NOT NULL, note TEXT, fs_status TEXT NOT NULL, revision INTEGER NOT NULL,
    dispatch TEXT UNIQUE, message_id TEXT, message_url TEXT, posted_ns INTEGER,
    result_id TEXT, result_status TEXT, result_text TEXT,
    pending_key TEXT, pending_body BLOB, pending_seq INTEGER, reply_cursor TEXT, candidate TEXT);
"""
COLUMNS = ("task", "assign_seq", "text", "phase", "note", "fs_status", "revision", "dispatch", "message_id",
           "message_url", "posted_ns", "result_id", "result_status", "result_text", "pending_key", "pending_body", "pending_seq",
           "reply_cursor", "candidate")


class Bridge:
    """One process per bridge state; the state lock excludes local duplicates."""

    def __init__(self, config: BridgeConfig, graph: Graph | None = None, clock=time.time_ns):
        self.config, self.clock = config, clock
        self.channel = config.load_channel()
        self.graph = graph or Graph(config)
        self.store = Store(config.state_dir / "bridge", config.binding(self.channel.channel))
        self.db = self.store.db
        try:
            self.db.executescript(SCHEMA)
            self.client = Client(self.channel, config.participant, config.state_dir / "channel")
        except BaseException:
            self.store.close()
            raise
        self.verified = False
        self.backoff = 0.0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.store.close()
        self.store.close()

    def rows(self, where: str = "1", args: tuple = ()) -> list[dict]:
        query = f"SELECT {','.join(COLUMNS)} FROM bridge_tasks WHERE {where} ORDER BY assign_seq"
        return [dict(zip(COLUMNS, row)) for row in self.db.execute(query, args)]

    def update(self, task: str, **fields) -> None:
        with self.db:
            self.db.execute(f"UPDATE bridge_tasks SET {','.join(f'{k}=?' for k in fields)} WHERE task=?", (*fields.values(), task))

    # -- fs-channel intake

    def policy(self, task: str, text: str) -> str | None:
        if not task.startswith(self.config.task_prefixes):
            return "task ID is not in the copilot365 task allowlist"
        if len(text) > self.config.max_task_chars:
            return f"assignment text exceeds {self.config.max_task_chars} characters"
        bad = [u for u in URL.findall(text) if not link_allowed(u.rstrip(".,;:)]}"), self.config.allowed_link_hosts)]
        if bad:
            return "assignment links must be https OneDrive/SharePoint links on allowed_link_hosts"
        return None

    def drain(self, pages: int = 100) -> bool:
        """Apply feed pages; True only when caught up with the visible HEAD."""
        for _ in range(pages):
            if self.ingest() < self.channel.page_size:
                return True
        return False

    def ingest(self) -> int:
        row = self.db.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()
        records = read_events(self.channel, self.config.participant, int(row[0]) if row else 0)
        for record in records:
            event = record["event"]
            with self.db:
                self._apply(event)
                self.db.execute("INSERT OR REPLACE INTO meta VALUES ('cursor',?)", (str(event["seq"]),))
        return len(records)

    def _apply(self, event: dict) -> None:
        seq, message = event["seq"], event["message"]
        if event["author"] == self.config.participant:
            row = self.db.execute("SELECT task,revision,pending_body FROM bridge_tasks WHERE pending_seq=?", (event["author_seq"],)).fetchone()
            if row:
                self.db.execute("UPDATE bridge_tasks SET pending_key=NULL,pending_body=NULL,pending_seq=NULL WHERE task=?", (row[0],))
                if not event["accepted"] and object_json(row[2])["revision"] == row[1]:
                    # Not a concurrent update: retrying would repeat the same rejection.
                    self.db.execute("UPDATE bridge_tasks SET phase='stuck',note=? WHERE task=?", (f"fs-channel rejected bridge status: {event['reason']}"[:500], row[0]))
        if not event["accepted"] or not isinstance(message, dict):
            return
        if message.get("kind") == "assign" and message.get("assignee") == self.config.participant:
            if self.channel.participant(event["author"]).role != "coordinator":
                return
            reason = self.policy(message["task"], message["text"])
            self.db.execute("INSERT OR IGNORE INTO bridge_tasks(task,assign_seq,text,phase,note,fs_status,revision) VALUES (?,?,?,?,?,?,?)",
                            (message["task"], seq, message["text"] if reason is None else "", "queued" if reason is None else "rejected", reason, "assigned", seq))
        elif message.get("kind") == "status":
            row = self.db.execute("SELECT phase FROM bridge_tasks WHERE task=?", (message["task"],)).fetchone()
            if row:
                status = message["status"]
                phase = row[0]
                if status in {"completed", "failed"}:
                    phase = "done"
                elif status == "cancelled":
                    phase = "cancelled"
                self.db.execute("UPDATE bridge_tasks SET fs_status=?,revision=?,phase=? WHERE task=?", (status, seq, phase, message["task"]))

    # -- fs-channel reporting

    def target(self, row: dict, now: int) -> tuple[str, str] | None:
        phase, note, mid = row["phase"], row["note"], row["message_id"]
        if phase == "rejected":
            return "failed", f"copilot365 bridge did not dispatch this task: {note}"
        if phase in ("posting", "ambiguous"):
            # A durable posting intent seen here is unresolved (crash or unknown
            # outcome); report it without needing a successful channel scan.
            return "blocked", ("Teams dispatch outcome is ambiguous after a crash or transport error. The bridge never reposts "
                               "automatically. Operator: inspect the Teams channel, then run fs-copilot365 resolve.")
        if phase == "posted":
            if now - row["posted_ns"] >= self.config.result_timeout * 1_000_000_000:
                return "blocked", (f"No valid AMP_RESULT within {self.config.result_timeout}s of Teams message {mid}. Cowork may be "
                                   "waiting for user approval or did not fire; external effects are unknown. A late result is still accepted."
                                   + (f" Last bridge note: {note}" if note else ""))
            return "running", f"Posted AMP_TASK as Teams message {mid} {row['message_url'] or ''}".strip() + "; awaiting AMP_RESULT from Cowork."
        if phase == "resulted":
            return row["result_status"], row["result_text"]
        return None

    def fit(self, message: dict) -> dict:
        def size(m):
            return len(canonical_json({"protocol": 1, "channel": self.channel.channel, "author": self.config.participant,
                                       "seq": self.channel.max_events, "message": m}))
        while size(message) > self.channel.max_message_bytes:
            text = message["text"]
            if len(text) <= 16:
                raise Rejected("status overhead exceeds fs-channel message limit")
            message = {**message, "text": text[:max(1, len(text) - (size(message) - self.channel.max_message_bytes) - 32)] + " [truncated]"}
        return message

    def recover_pending(self) -> None:
        # A crash after queue() but before recording its sequence: queue() is
        # idempotent, so recover the sequence before any feed intake can pass it.
        # An intent that never reached the client is dropped and regenerated, so
        # recovery cannot hit the client pending limit before sync() drains it.
        for row in self.rows("pending_key IS NOT NULL AND pending_seq IS NULL"):
            sent = self.client.db.execute("SELECT seq FROM sent WHERE id=?", (row["pending_key"],)).fetchone()
            if sent:
                self.update(row["task"], pending_seq=sent[0])
            else:
                self.update(row["task"], pending_key=None, pending_body=None)

    def report(self) -> int:
        queued = 0
        now = self.clock()
        free = self.channel.max_pending - len(self.client.pending())
        for row in self.rows("phase NOT IN ('queued','cancelled','done','stuck')"):
            if row["pending_seq"] is not None:
                continue
            if queued >= free:
                break  # Publish existing sends first; continue next round.
            if row["pending_key"] is None:
                wanted = self.target(row, now)
                current = row["fs_status"]
                if wanted is None or current == wanted[0] or current not in TRANSITIONS:
                    continue
                status, text = wanted
                if status not in TRANSITIONS[current]:
                    text = f"copilot365 bridge transition before reporting {status}."
                    status = "running"
                    if status not in TRANSITIONS[current]:
                        continue
                message = self.fit({"kind": "status", "task": row["task"], "revision": row["revision"], "status": status, "text": text})
                key = "c365-" + sha256_bytes(canonical_json([row["task"], status, row["revision"], message]))[:40]
                # Intent first: a crash before queue() replays these exact bytes.
                self.update(row["task"], pending_key=key, pending_body=canonical_json(message))
                row["pending_key"], row["pending_body"] = key, canonical_json(message)
            seq = self.client.queue(object_json(row["pending_body"]), row["pending_key"])
            self.update(row["task"], pending_seq=seq)
            queued += 1
        return queued

    # -- Teams

    READ_ERRORS = (GraphError, OSError, ValueError, RecursionError)

    def failed(self, errors: list, exc: Exception, where: str) -> None:
        if isinstance(exc, GraphError) and exc.retry_after:
            raise exc  # Throttled: stop the whole round and honor Retry-After.
        errors.append({"error": type(exc).__name__, "detail": f"{where}: {str(exc)[:200]}"})

    def teams(self, errors: list) -> dict:
        stats = {"posted": 0, "results": 0}
        if not self.verified:
            if self.graph.me() != self.config.bridge_user_id:
                raise ValueError("Graph token is not the configured bridge account")
            self.verified = True
        if self.rows(f"phase IN {ACTIVE}"):
            roots, scanned = [], False
            try:
                roots = self.graph.list_roots(self.config.scan_pages)
                self.reconcile(roots)  # A failed scan proves nothing about a post.
                scanned = True
            except self.READ_ERRORS as exc:
                self.failed(errors, exc, "root scan")
            root_results = self.candidates(roots, None)
            for row in self.rows(f"phase IN {ACTIVE}"):
                stats["results"] += self.collect(row, root_results.get(row["task"]), errors, scanned)
        # Apply every visible cancellation before a new external effect.
        if self.drain():
            active = self.db.execute(f"SELECT count(*) FROM bridge_tasks WHERE phase IN {ACTIVE}").fetchone()[0]
            queued = self.rows("phase='queued'")
            if queued and active < self.config.max_outstanding:
                stats["posted"] += self.post(queued[0])  # At most one external post per round.
        return stats

    def collect(self, row: dict, root_best: dict | None, errors: list, scanned: bool = True) -> int:
        """Select the earliest valid result across a complete reply sweep and roots.

        Reply pages resume from a persisted nextLink, so long threads make
        bounded progress per round; a result commits only after a full sweep.
        """
        task = row["task"]
        best = earliest(object_json(row["candidate"].encode()) if row["candidate"] else None, root_best)
        if row["message_id"] is None:
            complete, cursor = True, None
        else:
            cursor = row["reply_cursor"] or self.graph.replies_url(row["message_id"])
            try:
                for _ in range(self.config.reply_pages):
                    values, cursor = self.graph.page(cursor)
                    best = earliest(best, self.candidates(values, row["message_id"]).get(task))
                    if cursor is None:
                        break
            except self.READ_ERRORS as exc:
                # Restart this thread's sweep next round; never accept around it.
                self.update(task, reply_cursor=None, candidate=None, note=f"reply read failed: {type(exc).__name__} {str(exc)[:120]}")
                self.failed(errors, exc, f"replies for {task}")
                return 0
            complete = cursor is None
            if complete and (row["note"] or "").startswith("reply read failed"):
                self.update(task, note=None)
        if not complete or (best is not None and not scanned):
            # Without this round's root scan an earlier root result may be unseen;
            # keep the candidate and finish selection in a later round.
            self.update(task, reply_cursor=cursor, candidate=None if best is None else canonical_json(best).decode())
            return 0
        if best is None:
            if row["reply_cursor"] or row["candidate"]:
                self.update(task, reply_cursor=None, candidate=None)
            return 0
        self.update(task, phase="resulted", result_id=best["id"], result_status=best["status"], result_text=best["text"],
                    reply_cursor=None, candidate=None)
        return 1

    def html(self, row: dict) -> str:
        e = html.escape
        body = "<br>".join(e(line) for line in row["text"].splitlines())
        task, dispatch = row["task"], row["dispatch"]
        return (f'<p>AMP_TASK v1 <at id="0">{e(self.config.responder_display_name)}</at></p>'
                f"<p>task: {task}<br>dispatch: {dispatch}<br>fs-channel: {self.channel.channel}</p>"
                f"<p>Reply in this thread. First line exactly: AMP_RESULT {task} {dispatch} completed (or failed). "
                "Then a short summary. Share documents only as OneDrive/SharePoint links.</p><p>---</p>"
                f"<p>{body}</p>")

    def post(self, row: dict) -> int:
        dispatch = row["dispatch"] or secrets.token_hex(16)
        # Durable intent before the external effect: never repost automatically.
        self.update(row["task"], phase="posting", dispatch=dispatch, note=None)
        row = {**row, "dispatch": dispatch}
        try:
            message = self.graph.post_message(self.html(row))
            mid = message.get("id")
            if not isinstance(mid, str) or not MESSAGE_ID.fullmatch(mid):
                raise ValueError("Graph response lacks a message ID")
        except NotSent:
            self.update(row["task"], phase="queued")
            raise
        except GraphError as exc:
            if exc.definite:
                if exc.status in {400, 413, 415, 422}:
                    self.update(row["task"], phase="rejected", note=f"Teams rejected the message (HTTP {exc.status} {exc.code})")
                    return 0
                self.update(row["task"], phase="queued")
            else:
                self.update(row["task"], phase="ambiguous", note=f"POST outcome unknown: {exc}")
            raise
        except (OSError, ValueError, RecursionError) as exc:
            self.update(row["task"], phase="ambiguous", note=f"POST outcome unknown: {type(exc).__name__}")
            raise
        self.update(row["task"], phase="posted", message_id=mid, message_url=teams_url(message), posted_ns=self.clock())
        return 1

    def reconcile(self, roots: list[dict]) -> None:
        unresolved = self.rows("phase IN ('posting','ambiguous') AND message_id IS NULL")
        if not unresolved:
            return
        found = {}
        for message in roots:
            if not isinstance(message, dict) or message.get("replyToId") or self.sender(message) != self.config.bridge_user_id:
                continue
            try:
                text, _, _ = teams_text(message.get("body"))
            except Rejected:
                continue
            header = "\n".join(text.split("\n")[:4])
            mid = message.get("id")
            for dispatch in DISPATCH.findall(header):
                if isinstance(mid, str) and MESSAGE_ID.fullmatch(mid):
                    found.setdefault(dispatch, message)
        for row in unresolved:
            message = found.get(row["dispatch"])
            if message:
                self.update(row["task"], phase="posted", message_id=message["id"], message_url=teams_url(message), posted_ns=self.clock())
            elif row["phase"] == "posting":
                self.update(row["task"], phase="ambiguous", note="AMP_TASK not found in recent channel messages")

    def sender(self, message: dict) -> str | None:
        user = (message.get("from") or {}).get("user") if isinstance(message.get("from"), dict) else None
        value = user.get("id") if isinstance(user, dict) else None
        return value.lower() if isinstance(value, str) else None

    def candidates(self, messages: list, reply_to: str | None) -> dict[str, dict]:
        """Earliest valid AMP_RESULT per active task among these messages; no commit."""
        best: dict[str, dict] = {}
        for message in messages:
            if not isinstance(message, dict) or not isinstance(message.get("id"), str) or not MESSAGE_ID.fullmatch(message["id"]):
                continue
            if message.get("messageType") != "message" or message.get("deletedDateTime") is not None:
                continue
            if self.sender(message) != self.config.responder_user_id or message.get("replyToId") != reply_to:
                continue
            identity = message.get("channelIdentity")
            if identity is not None and (not isinstance(identity, dict) or str(identity.get("teamId", "")).lower() != self.config.team_id or identity.get("channelId") != self.config.channel_id):
                continue
            try:
                text, links, header = teams_text(message.get("body"))
            except Rejected:
                continue
            match = RESULT.fullmatch(" ".join(header.split()))
            if not match:
                continue
            task, dispatch, status = match.groups()
            row = next(iter(self.rows("task=? AND dispatch=?", (task, dispatch))), None)
            if not row or row["phase"] not in ACTIVE or (reply_to is not None and row["message_id"] != reply_to):
                continue  # Unknown, late, or misthreaded result.
            attachments = message.get("attachments")
            for attachment in attachments[:MAX_LINKS] if isinstance(attachments, list) else ():
                # Files shared in Teams are reference attachments to OneDrive/SharePoint.
                if isinstance(attachment, dict) and attachment.get("contentType") == "reference" and isinstance(attachment.get("contentUrl"), str):
                    links.append(attachment["contentUrl"])
            candidate = {"id": message["id"], "created": timestamp(message.get("createdDateTime")), "status": status,
                         "text": self.result_text(message, status, "\n".join(text.split("\n")[1:]).strip(), links)}
            best[task] = earliest(best.get(task), candidate)
        return best

    def result_text(self, message: dict, status: str, summary: str, links: list[str]) -> str:
        hosts = self.config.allowed_link_hosts
        allowed, withheld = [], 0
        for url in links:
            if link_allowed(url, hosts):
                if url not in allowed and len(allowed) < MAX_LINKS:
                    allowed.append(url)
            else:
                withheld += 1
        summary = URL.sub(lambda m: m.group(0) if link_allowed(m.group(0).rstrip(".,;:)]}"), hosts) else "[link withheld]", summary)
        if len(summary) > self.config.max_result_chars:
            summary = summary[:self.config.max_result_chars] + " [truncated]"
        text = f"AMP_RESULT {status} from Cowork (Teams message {message['id']} {teams_url(message)}".rstrip() + ").\n" + (summary or "(no summary)")
        if allowed:
            text += "\nLinks:\n" + "\n".join(f"- {u}" for u in allowed)
        if withheld:
            text += f"\n{withheld} non-allowlisted link(s) withheld; inspect the Teams message."
        return text

    # -- round

    def once(self) -> dict:
        errors = []
        self.recover_pending()
        self.client.sync()
        self.drain()
        stats = {"posted": 0, "results": 0}
        if time.monotonic() >= self.backoff:
            try:
                stats = self.teams(errors)
            except self.READ_ERRORS as exc:
                if isinstance(exc, GraphError) and exc.retry_after:
                    self.backoff = time.monotonic() + exc.retry_after
                errors.append({"error": type(exc).__name__, "detail": str(exc)[:200]})
        reported = self.report()
        self.client.sync()
        self.drain()
        return {**stats, "reported": reported, "errors": errors, "phases": self.phases()}

    def phases(self) -> dict:
        return dict(self.db.execute("SELECT phase,count(*) FROM bridge_tasks GROUP BY phase"))

    def status(self) -> dict:
        row = self.db.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()
        keep = ("task", "assign_seq", "phase", "note", "fs_status", "revision", "dispatch", "message_id", "result_id", "result_status", "pending_seq")
        return {"cursor": int(row[0]) if row else 0, "pending_channel_sends": self.client.pending(),
                "tasks": [{k: r[k] for k in keep} for r in self.rows()]}

    def resolve(self, task: str, adopt: str | None = None, repost: bool = False, abandon: bool = False) -> dict:
        row = next(iter(self.rows("task=?", (task,))), None)
        if row is None or sum((adopt is not None, repost, abandon)) != 1:
            raise ValueError("unknown task or choose exactly one of --adopt/--repost/--abandon")
        if abandon:
            if row["phase"] not in ("queued", *ACTIVE):
                raise ValueError("only queued or dispatched tasks can be abandoned")
            self.update(task, phase="rejected", note="abandoned by operator; Teams/Cowork effects may still occur")
        elif row["phase"] not in ("posting", "ambiguous") or row["message_id"]:
            raise ValueError("only an unresolved ambiguous dispatch can be adopted or reposted")
        elif repost:
            # Same dispatch ID: a duplicate post shares one first-wins result slot.
            self.update(task, phase="queued", note="operator asserted the AMP_TASK was not posted")
        else:
            if not MESSAGE_ID.fullmatch(adopt):
                raise ValueError("invalid Teams message ID")
            message = self.graph.get_message(adopt)
            text, _, _ = teams_text(message.get("body"))
            if message.get("id") != adopt or message.get("replyToId") or self.sender(message) != self.config.bridge_user_id or row["dispatch"] not in DISPATCH.findall("\n".join(text.split("\n")[:4])):
                raise ValueError("message is not this task's AMP_TASK from the bridge account")
            self.update(task, phase="posted", message_id=adopt, message_url=teams_url(message), posted_ns=self.clock(), note="adopted by operator")
        return next(iter(self.rows("task=?", (task,))))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="validate config/secret; --online also checks Graph identity and channel read")
    check.add_argument("--online", action="store_true")
    commands.add_parser("login", help="device-code sign-in of the dedicated bridge account")
    commands.add_parser("once")
    commands.add_parser("run")
    commands.add_parser("status", help="private bridge state; stop the bridge first")
    resolve = commands.add_parser("resolve", help="operator decision for an ambiguous dispatch")
    resolve.add_argument("task")
    choice = resolve.add_mutually_exclusive_group(required=True)
    choice.add_argument("--adopt", metavar="MESSAGE_ID")
    choice.add_argument("--repost", action="store_true")
    choice.add_argument("--abandon", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = BridgeConfig.load(args.config)
        if args.command == "check":
            channel = config.load_channel()
            result = {"config": "ok", "fs_channel": channel.channel, "participant": config.participant}
            if config.secret_file.exists():
                read_secret(config.secret_file)
                result["secret"] = "ok"
            else:
                result["secret"] = "missing; run login"
            if args.online:
                graph = Graph(config)
                if graph.me() != config.bridge_user_id:
                    raise ValueError("Graph token is not the configured bridge account")
                graph.request("GET", f"{graph.channel_path}?$top=1")
                result["graph"] = "identity and channel read ok; posting is not tested"
            print(json.dumps(result))
        elif args.command == "login":
            config.load_channel()
            print(json.dumps({"signed_in": device_login(config), "secret_file": str(config.secret_file)}))
        else:
            with Bridge(config) as bridge:
                if args.command == "status":
                    print(json.dumps(bridge.status()))
                elif args.command == "resolve":
                    print(json.dumps({k: v for k, v in bridge.resolve(args.task, args.adopt, args.repost, args.abandon).items() if k not in {"pending_body", "result_text"}}))
                elif args.command == "once":
                    result = bridge.once()
                    print(json.dumps(result))
                    return 1 if result["errors"] else 0
                else:
                    while True:
                        try:
                            result = bridge.once()
                            print(json.dumps({k: result[k] for k in ("posted", "results", "reported", "errors", "phases")}), flush=True)
                        except (OSError, ValueError, sqlite3.Error, RecursionError) as exc:
                            print(json.dumps({"degraded": type(exc).__name__}), file=sys.stderr, flush=True)
                        time.sleep(config.poll_seconds)
        return 0
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError, TypeError, RecursionError, sqlite3.Error, GraphError) as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)[:300]}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
