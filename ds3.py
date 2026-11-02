#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DS3 — faster successor of DS2 Studio (Duck.ai Chat CLI API local web UI).
=========================================================================

Single-file Flask app. Same feature set as ds2.py plus the speed/robustness/UI
items from ds3/IMPROVEMENTS.md (see the AI HANDOFF block below).

Run:      python ds3.py            (opens http://127.0.0.1:7862)
Options:  python ds3.py --port 7862 --workspace ./workspace --max-turns 15

Dependencies: flask, requests. Optional: markdown-it-py (markdown), pygments
(syntax highlighting), orjson (serialisation) — all degrade gracefully.

Upstream API (CLI v1.9.9, base http://127.0.0.1:8989/api/v1):
  POST /chat          {"message": "...", "model": "gpt-6-luna"}
                      -> data.response
  POST /chat/stream   same body, SSE:
                        event: start  data: {"model": "..."}
                        event: delta  data: {"delta": "..."}          (many)
                        event: done   data: {"model","message_id",
                                             "metadata":{"processing_time_ms",
                                             "tokens_estimate",
                                             "conversation_count"}}
  GET  /models        -> data.models[] + data.current_model
  GET  /session       -> data.current_model, data.session_id (health fallback)
  GET  /health        -> 404 on this build (fallback probes /session)
  GET  /history       -> data.messages[]   (DELETE clears the CLI session)
"""

# =============================================================================
# AI HANDOFF / CONTINUATION NOTES  — read before changing anything
# =============================================================================
#
# WHAT THIS IS
#   ds3.py is a drop-in faster successor to ds2.py. It keeps every ds2 feature
#   (all routes, CLI command mappings, exact log strings, animations, sidebar,
#   workspace, sessions, export) and adds the speed/robustness/UI items marked
#   below. Single file on purpose: server + embedded UI, no build step.
#
# GROUND RULES (owner, do not violate)
#   - Additive only: never remove a route, command, log format or animation.
#   - No new restrictions: it is a personal tool. Caps are constants/flags,
#     big files truncate with a warning, binary previews are allowed.
#   - Keep the exact strings:
#       "🔄 [LOG] {ts} | Session cleared. New ID: {sid}. History reset. Ready."
#       "⚠️ [LOG] {ts} | Upstream 429/Rate Limit detected. The CLI is still in
#        cooldown. Please wait or run /clear in the main CLI terminal."
#   - No Gradio, no CAPS prompt budgeting, no action-JSON sandbox, no
#     auto-continue loops. Manual buttons only.
#
# ARCHITECTURE MAP
#   constants ................. configuration, all overridable via CLI flags
#   state ..................... _SESSIONS (memory) + data/sessions/*.json (disk)
#   check_health() ............ /health -> 404 -> /api/v1/session, hard 3 s
#                               deadline, 2 s result cache (force=True skips)
#   compose_prompt() .......... search block -> attachments -> transcript ->
#                               new message; drops oldest turns at MAX_PROMPT_CHARS
#   /api/chat/stream .......... SSE proxy of upstream /chat/stream, persists the
#                               reply (incl. partial on client abort)
#   /api/chat ................. non-streaming JSON path (fallback, tests,
#                               edit_last support)
#   /api/retry ................ regenerate the last assistant turn (JSON)
#   /api/search ............... DDG scrape + 10 min LRU cache (prefetch)
#   /api/save ................. user-initiated "save reply to workspace"
#   /api/{session,sessions,tree,workspace,read,url,models,health}
#   INDEX_HTML ................ whole UI (CSS + JS). Client protocol:
#                               POST /api/chat/stream -> SSE events
#                               "delta" {delta}, "final" {html, ...}, "error" {log}
#   UI state .................. S.messages[], S.pending{files,search,block},
#                               S.abort (AbortController), S.pinned (scroll)
#
# IMPLEMENTED FROM IMPROVEMENTS.md
#   S1 streaming replies + Stop          S2 incremental DOM rendering
#   S3 search prefetch + cache           S4 shared requests.Session
#   S5 health cache/single-flight        S6 models cache (60 s)
#   S7 singleton MarkdownIt              R1 disk-persisted sessions
#   R2 chat/stream deadlines             R3 draft restore on failure
#   R4 better error taxonomy             U1 Pygments highlighting
#   U2 stick-to-bottom + jump pill       U3 toasts
#   U4/N6 hover metadata + latency       U5 resizable sidebar + localStorage
#   U6 light/dark theme                  U7 edit & resend last user message
#   U8 drag-drop / file picker           N1 save reply to workspace
#   N5 context budget gauge              N11 auto-recovery polling
#   L1 caps configurable                 L2 big files truncate (warn)
#   L3 binary preview allowed            L4 unlimited history option
#
# STILL TODO (next agent: pick from ds3/IMPROVEMENTS.md, keep it additive)
#   R5 replay from /history, R6 per-session locks, R7 resume stream,
#   R8 optional-dep audit, R9 429 countdown button, R10 more flags,
#   U9 session titles/rename/delete UI, U10 code max-height, U11 quick actions,
#   U12 keyboard shortcuts, N2 full-text session search, N3 snippets,
#   N4 system-prompt field, N7/N8 json/html export+import, N9 Continue button,
#   N10 log viewer, N12 starred replies.
#
# HOW TO EXTEND SAFELY
#   1. python -m py_compile ds3.py
#   2. python ds3.py  ->  curl /api/health, /api/models, POST /api/chat
#   3. streaming: curl -sN -X POST /api/chat/stream -H "Content-Type:
#      application/json" -d '{"message":"hi"}'  (expect event: delta... final)
#   4. UI files live only in INDEX_HTML; server state is thread-locked.
#   Data lives in ds3/data/sessions/*.json (delete freely; self-healing).
# =============================================================================

from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import secrets
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request

try:
    from markdown_it import MarkdownIt
except ImportError:
    MarkdownIt = None

try:
    from pygments import highlight
    from pygments.formatters import HtmlFormatter
    from pygments.lexers import TextLexer, get_lexer_by_name, guess_lexer
    PYGMENTS = True
except ImportError:
    PYGMENTS = False


# =============================================================================
# Configuration (all overridable from the command line, see __main__)
# =============================================================================

CLI_BASE = "http://127.0.0.1:8989"
API_BASE = CLI_BASE + "/api/v1"

APP_HOST = "127.0.0.1"
APP_PORT = 7862                 # ds2 uses 7861, run both side by side

HEALTH_TIMEOUT = 3.0            # hard wall-clock deadline for health probes
HEALTH_CACHE_TTL = 2.0          # page load + pre-send + server gate share one probe
CHAT_TIMEOUT = 180.0            # per-read timeout for the upstream stream
CHAT_DEADLINE = 240.0           # overall wall-clock cap for one request
STREAM_CONNECT_TIMEOUT = 6.0

MAX_TURNS = 15                  # 0 = send full history (disk keeps everything)
MAX_PROMPT_CHARS = 9900         # API message limit is 10000
ATTACH_MAX_CHARS = 60000
MAX_FILE_READ = 200_000         # preview truncates beyond this (warning, not error)
PREVIEW_HARD_CAP = 5_000_000    # ?full=1 ceiling
TREE_DEPTH = 3
TREE_MAX_PER_DIR = 200
MAX_SESSIONS = 200              # disk-backed, display shows newest 15
MODELS_TTL = 60.0
SEARCH_TTL = 600.0
SEARCH_CACHE_MAX = 50

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SESS_DIR = DATA_DIR / "sessions"
EXPORT_DIR = BASE_DIR / "workspace" / "exports"
DEFAULT_WORKSPACE = BASE_DIR / "workspace"

for _d in (DATA_DIR, SESS_DIR, EXPORT_DIR, DEFAULT_WORKSPACE):
    try:
        _d.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 DS3/1.0")

OK_BADGE = "🟢 CLI Connected"
LIMITED_BADGE = "🟡 Rate Limited: Wait 9h"
OFFLINE_BADGE = "🔴 CLI Offline"

DEFAULT_MODELS = [
    {"id": "gpt-6-luna", "name": "GPT-6 Luna", "description": "Duck.ai's current general-purpose model"},
    {"id": "gpt-5.6-luna", "name": "GPT-5.6 Luna", "description": "Previous Luna model"},
    {"id": "gpt-5.4-nano", "name": "GPT-5.4 Nano", "description": "Fast, lightweight model"},
    {"id": "gpt-5.4-mini", "name": "GPT-5.4 Mini", "description": "Fast general-purpose model"},
    {"id": "claude-haiku-4-5", "name": "Claude Haiku 4.5", "description": "Anthropic's fast conversational model"},
    {"id": "mistral-small-2603", "name": "Mistral Small 4", "description": "Mistral's efficient model"},
    {"id": "tinfoil/gpt-oss-120b", "name": "GPT OSS 120B", "description": "Open-weight reasoning model"},
    {"id": "tinfoil/gemma4-31b", "name": "Gemma 4 31B", "description": "Open model for general conversations"},
]


# =============================================================================
# Shared HTTP session (connection reuse) + deadline helper
# =============================================================================

HTTP = requests.Session()
_ADAPTER = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
HTTP.mount("http://", _ADAPTER)
HTTP.mount("https://", _ADAPTER)

_DEADLINE_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ds3-deadline")


def _get_with_deadline(url: str, deadline: float):
    """GET with a true wall-clock deadline (requests timeouts are per-op)."""
    fut = _DEADLINE_POOL.submit(HTTP.get, url, timeout=deadline)
    try:
        return fut.result(timeout=deadline + 0.5)
    except FutureTimeout:
        fut.cancel()
        raise requests.exceptions.Timeout(f"{url} exceeded {deadline:.0f}s")


# =============================================================================
# State: sessions in memory + on disk
# =============================================================================

_LOCK = threading.Lock()
_SESSIONS: dict[str, dict] = {}      # sid -> {"messages": [...], "created", "active", "title"}
_ORDER: list[str] = []               # creation order
_CURRENT: str | None = None

WORKSPACE = DEFAULT_WORKSPACE


# =============================================================================
# Small helpers
# =============================================================================

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _new_session_id() -> str:
    return f"ds3_{int(time.time())}_{secrets.token_hex(3)}"


def _h(state: str, badge: str, send: bool, detail: str) -> dict:
    return {"state": state, "badge": badge, "send": send, "detail": detail}


def _rate_log() -> str:
    return (f"⚠️ [LOG] {_ts()} | Upstream 429/Rate Limit detected. The CLI is "
            f"still in cooldown. Please wait or run /clear in the main CLI terminal.")


def _offline_log() -> str:
    return (f"🔴 [LOG] {_ts()} | CLI Offline: cannot reach {CLI_BASE}. "
            f"Start the Duck CLI, then click the badge to retry.")


# =============================================================================
# Markdown rendering: escaped text -> markdown-it (singleton) -> Pygments blocks
# =============================================================================

_MD_LOCK = threading.Lock()
_MD = None

_LANG_BLOCK_RE = re.compile(
    r'<pre><code(?: class="language-([^"]*)")?>(.*?)</code></pre>', re.S)


def _md_instance():
    global _MD
    if _MD is None:
        if MarkdownIt is None:
            return None
        md = MarkdownIt("commonmark", {"html": False})
        md.enable("table")
        md.enable("strikethrough")
        _MD = md
    return _MD


def _highlight_block(match):
    lang = (match.group(1) or "").strip()
    code = html_lib.unescape(match.group(2))
    if not PYGMENTS:
        return match.group(0)
    try:
        lexer = get_lexer_by_name(lang) if lang else guess_lexer(code)
    except Exception:
        lexer = TextLexer()
    try:
        body = highlight(code, lexer, HtmlFormatter(nowrap=True))
    except Exception:
        return match.group(0)
    return f'<pre class="hl"><code>{body}</code></pre>'


def render_md(text: str) -> str:
    """Safe (escaped first) markdown with optional Pygments highlighting."""
    safe = (str(text)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))
    with _MD_LOCK:
        md = _md_instance()
        if md is None:
            return "<pre>" + safe + "</pre>"
        try:
            out = md.render(safe)
        except Exception:
            return "<pre>" + safe + "</pre>"
    if PYGMENTS:
        try:
            out = _LANG_BLOCK_RE.sub(_highlight_block, out)
        except Exception:
            pass
    return out


# =============================================================================
# Session persistence (ds3/data/sessions/*.json — atomic writes)
# =============================================================================

def _session_file(sid: str) -> Path:
    return SESS_DIR / f"{sid}.json"


def _save_session(sid: str) -> None:
    entry = _SESSIONS.get(sid)
    if entry is None:
        return
    payload = {
        "session_id": sid,
        "created": entry.get("created", _ts()),
        "active": entry.get("active", _ts()),
        "title": entry.get("title", ""),
        "messages": entry.get("messages", []),
    }
    try:
        tmp = SESS_DIR / f"{sid}.{os.getpid()}.tmp"
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, _session_file(sid))
    except OSError:
        pass


def _load_sessions() -> None:
    global _CURRENT
    try:
        files = sorted(SESS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
    except OSError:
        return
    for path in files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        sid = str(data.get("session_id") or path.stem)
        msgs = data.get("messages")
        if not isinstance(msgs, list):
            continue
        _SESSIONS[sid] = {
            "messages": msgs,
            "created": data.get("created") or _ts(),
            "active": data.get("active") or _ts(),
            "title": data.get("title") or "",
        }
        _ORDER.append(sid)
    if _ORDER:
        _CURRENT = _ORDER[-1]


def _ensure_session() -> str:
    """Create the current session if needed. Caller must hold _LOCK."""
    global _CURRENT
    if _CURRENT is None or _CURRENT not in _SESSIONS:
        _CURRENT = _new_session_id()
        _SESSIONS[_CURRENT] = {"messages": [], "created": _ts(), "active": _ts(), "title": ""}
        _ORDER.append(_CURRENT)
        _prune_sessions()
    return _CURRENT


def _prune_sessions() -> None:
    """Keep the in-memory list bounded; disk keeps everything. Caller: _LOCK."""
    while len(_ORDER) > MAX_SESSIONS:
        evicted = False
        for sid in list(_ORDER):
            if sid != _CURRENT:
                _SESSIONS.pop(sid, None)
                _ORDER.remove(sid)
                evicted = True
                break
        if not evicted:
            break


def _trim(messages: list) -> list:
    if MAX_TURNS and len(messages) > MAX_TURNS * 2:
        return messages[-MAX_TURNS * 2:]
    return messages


_load_sessions()


# =============================================================================
# Health: hard 3 s deadline + 2 s cache (single-flight-ish)
# =============================================================================

_HEALTH_LOCK = threading.Lock()
_HEALTH_CACHE = {"at": 0.0, "value": None}


def _probe_health() -> dict:
    try:
        r = _get_with_deadline(f"{CLI_BASE}/health", HEALTH_TIMEOUT)
    except requests.exceptions.RequestException:
        return _h("offline", OFFLINE_BADGE, False, f"Cannot reach {CLI_BASE}")

    if r.status_code == 404:
        try:
            r = _get_with_deadline(f"{API_BASE}/session", HEALTH_TIMEOUT)
        except requests.exceptions.RequestException:
            return _h("offline", OFFLINE_BADGE, False, f"Cannot reach {API_BASE}/session")

    status = r.status_code
    if status == 429:
        return _h("limited", LIMITED_BADGE, False, "HTTP 429 Too Many Requests")
    if status == 503:
        return _h("limited", LIMITED_BADGE, False, "HTTP 503 (bootstrap failed / rate limit)")
    if status >= 500:
        return _h("limited", LIMITED_BADGE, False, f"HTTP {status} from the CLI")
    if 200 <= status < 300:
        return _h("ok", OK_BADGE, True, f"HTTP {status} (verified)")
    return _h("ok", OK_BADGE, True, f"reachable (probe HTTP {status})")


def check_health(force: bool = False) -> dict:
    now = time.monotonic()
    with _HEALTH_LOCK:
        cached = _HEALTH_CACHE["value"]
        if not force and cached and now - _HEALTH_CACHE["at"] < HEALTH_CACHE_TTL:
            return cached
    value = _probe_health()
    with _HEALTH_LOCK:
        _HEALTH_CACHE["at"] = time.monotonic()
        _HEALTH_CACHE["value"] = value
    return value


# =============================================================================
# Prompt composition
# =============================================================================

def compose_prompt(context, new_content, attachments, search_block):
    notices: list[str] = []
    att_blocks: list[str] = []
    for att in attachments or []:
        if not isinstance(att, dict):
            continue
        name = str(att.get("name") or "file")[:80]
        content = str(att.get("content") or "")
        if len(content) > ATTACH_MAX_CHARS:
            content = content[:ATTACH_MAX_CHARS] + "\n…[attachment truncated]"
            notices.append(f"📎 [LOG] {_ts()} | Attachment '{name}' truncated to {ATTACH_MAX_CHARS} chars.")
        att_blocks.append(f"[Attached file: {name}]\n{content}")

    def build(k: int) -> str:
        segs: list[str] = []
        if search_block:
            segs.append(str(search_block))
        segs.extend(att_blocks)
        if k:
            lines = ["(Earlier conversation, oldest first)"]
            for m in context[-k:]:
                role = "User" if m.get("role") == "user" else "Assistant"
                lines.append(f"{role}:")
                lines.append(str(m.get("content") or ""))
            segs.append("\n".join(lines))
        segs.append(str(new_content))
        return "\n\n".join(segs)

    k = len(context or [])
    prompt = build(k)
    while len(prompt) > MAX_PROMPT_CHARS and k > 0:
        k -= 1
        prompt = build(k)
    if len(prompt) > MAX_PROMPT_CHARS:
        prompt = "…\n" + prompt[len(prompt) - MAX_PROMPT_CHARS:]
    return prompt[:MAX_PROMPT_CHARS], notices


# =============================================================================
# Upstream: rate-limit detection, reply extraction, JSON call, SSE proxy
# =============================================================================

def _is_rate_limited(status: int, body=None) -> bool:
    if status in (429, 503):
        return True
    if status == 200 and isinstance(body, dict) and body.get("success") is False:
        blob = json.dumps(body.get("error") or {}).lower()
        if any(k in blob for k in ("rate_limit", "rate limit", "too many requests", "bootstrap failed")):
            return True
    return False


def _extract_reply(body):
    if not isinstance(body, dict):
        return None
    data = body.get("data")
    if isinstance(data, dict):
        for key in ("response", "reply", "message", "text", "content"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                return val
    for key in ("response", "reply", "text", "content"):
        val = body.get(key)
        if isinstance(val, str) and val.strip():
            return val
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        msg = (choices[0] or {}).get("message") or {}
        val = msg.get("content")
        if isinstance(val, str) and val.strip():
            return val
    return None


def _failure_health(kind: str) -> dict:
    if kind == "rate":
        return _h("limited", LIMITED_BADGE, False, "429/503 from upstream")
    if kind == "offline":
        return _h("offline", OFFLINE_BADGE, False, f"Cannot reach {CLI_BASE}")
    return _h("ok", OK_BADGE, True, "last request failed for another reason")


def _upstream_json(prompt: str, model: str | None):
    """Non-streaming POST /chat. Returns (kind, payload)."""
    payload = {"message": prompt}
    if model:
        payload["model"] = model
    fut = _DEADLINE_POOL.submit(HTTP.post, f"{API_BASE}/chat", json=payload, timeout=CHAT_TIMEOUT)
    try:
        r = fut.result(timeout=CHAT_DEADLINE)
    except FutureTimeout:
        fut.cancel()
        return "error", f"⏱️ [LOG] {_ts()} | Upstream exceeded {int(CHAT_DEADLINE)}s — aborted."
    except requests.exceptions.ConnectionError:
        return "offline", _offline_log()
    except requests.exceptions.RequestException as exc:
        return "error", f"🛑 [LOG] {_ts()} | Upstream request failed: {exc}"

    body = None
    try:
        body = r.json()
    except ValueError:
        body = None
    if _is_rate_limited(r.status_code, body):
        return "rate", _rate_log()
    if r.status_code != 200:
        return "error", f"🛑 [LOG] {_ts()} | Upstream HTTP {r.status_code}: {(r.text or '')[:180]}"
    reply = _extract_reply(body)
    if reply is None:
        if isinstance(body, dict) and body.get("success") is False:
            err = body.get("error") or {}
            msg = err.get("message") or body.get("message") or "unknown error"
            return "error", f"🛑 [LOG] {_ts()} | Upstream error: {msg}"
        return "error", f"🛑 [LOG] {_ts()} | Unrecognised upstream response: {(r.text or '')[:180]}"
    return "ok", reply


def _upstream_stream(prompt: str, model: str | None):
    """Generator over upstream SSE: yields ("delta", text) / ("done", metadata).
    Errors are raised as requests exceptions; rate limits raise RuntimeError."""
    payload = {"message": prompt}
    if model:
        payload["model"] = model
    r = HTTP.post(f"{API_BASE}/chat/stream", json=payload, stream=True,
                  timeout=(STREAM_CONNECT_TIMEOUT, CHAT_TIMEOUT))
    try:
        if _is_rate_limited(r.status_code):
            raise RuntimeError("rate")
        if r.status_code != 200:
            snippet = (r.text or "")[:150]
            raise RuntimeError(f"http:{r.status_code}:{snippet}")
        event = ""
        for line in r.iter_lines(decode_unicode=True):
            if line is None:
                continue
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                raw = line[5:].strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except ValueError:
                    continue
                if event == "delta" and isinstance(obj, dict) and "delta" in obj:
                    yield "delta", str(obj.get("delta") or "")
                elif event == "done":
                    yield "done", (obj.get("metadata") or {})
    finally:
        try:
            r.close()
        except Exception:
            pass


# =============================================================================
# Web search (keyless DuckDuckGo) with a small LRU/TTL cache
# =============================================================================

_SEARCH_LOCK = threading.Lock()
_SEARCH_CACHE: dict[str, tuple[float, str]] = {}


def _strip_html(raw: str) -> str:
    raw = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", raw)
    raw = re.sub(r"(?s)<!--.*?-->", " ", raw)
    raw = re.sub(r"(?s)<[^>]+>", " ", raw)
    text = html_lib.unescape(raw)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _unwrap_ddg(href: str) -> str:
    if href.startswith("//"):
        href = "https:" + href
    if "uddg=" in href:
        try:
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
            if qs.get("uddg"):
                href = qs["uddg"][0]
        except Exception:
            pass
    return href


def ddg_search(query: str) -> str | None:
    headers = {"User-Agent": UA}
    page = ""
    try:
        r = HTTP.post("https://lite.duckduckgo.com/lite/", data={"q": query},
                      headers=headers, timeout=STREAM_CONNECT_TIMEOUT)
        if r.status_code == 200 and r.text:
            page = r.text
    except requests.exceptions.RequestException:
        pass
    if not page:
        try:
            r = HTTP.get("https://html.duckduckgo.com/html/", params={"q": query},
                         headers=headers, timeout=STREAM_CONNECT_TIMEOUT)
            if r.status_code == 200 and r.text:
                page = r.text
        except requests.exceptions.RequestException:
            page = ""
    if not page:
        return None

    seen, results = set(), []
    for m in re.finditer(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', page, re.S | re.I):
        href = _unwrap_ddg(m.group(1))
        title = _strip_html(m.group(2)).strip()
        if len(title) < 4 or not href.lower().startswith("http"):
            continue
        low = href.lower()
        if "duckduckgo.com" in low:
            continue
        if low.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".ico")):
            continue
        if href in seen:
            continue
        seen.add(href)
        results.append((title, href))
        if len(results) >= 5:
            break
    if not results:
        return None
    lines = [f'Web search results for "{query}":']
    for i, (title, url) in enumerate(results, 1):
        lines.append(f"{i}. {title}")
        lines.append(f"   {url}")
    return "\n".join(lines)


def cached_search(query: str) -> str | None:
    now = time.monotonic()
    with _SEARCH_LOCK:
        hit = _SEARCH_CACHE.get(query)
        if hit and now - hit[0] < SEARCH_TTL:
            return hit[1]
    block = ddg_search(query)
    if block:
        with _SEARCH_LOCK:
            if len(_SEARCH_CACHE) >= SEARCH_CACHE_MAX:
                oldest = min(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0])
                _SEARCH_CACHE.pop(oldest, None)
            _SEARCH_CACHE[query] = (now, block)
    return block


# =============================================================================
# Workspace helpers
# =============================================================================

def build_tree(root: Path, level: int = 0, prefix: str = "") -> list:
    out: list[dict] = []
    try:
        entries = sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except (PermissionError, OSError):
        return out
    for p in entries:
        if len(out) >= TREE_MAX_PER_DIR:
            break
        rel = f"{prefix}{p.name}"
        try:
            if p.is_dir():
                children = build_tree(p, level + 1, rel + "/") if level + 1 < TREE_DEPTH else []
                out.append({"name": p.name, "type": "dir", "path": rel, "children": children})
            elif p.is_file():
                out.append({"name": p.name, "type": "file", "path": rel, "size": p.stat().st_size})
        except (PermissionError, OSError):
            continue
    return out


# =============================================================================
# Flask app
# =============================================================================

app = Flask(__name__)


@app.get("/")
def index():
    return INDEX_HTML.replace("/*PYGMENTS_CSS*/", PYGMENTS_CSS)


@app.get("/api/health")
def api_health():
    force = request.args.get("force") in ("1", "true", "yes")
    return jsonify(check_health(force=force))


_MODELS_LOCK = threading.Lock()
_MODELS_CACHE = {"at": 0.0, "payload": None}


@app.get("/api/models")
def api_models():
    now = time.monotonic()
    with _MODELS_LOCK:
        cached = _MODELS_CACHE["payload"]
        if cached and now - _MODELS_CACHE["at"] < MODELS_TTL:
            return jsonify(cached)
    payload = {"ok": True, "models": DEFAULT_MODELS, "current_model": None, "fallback": True}
    try:
        r = HTTP.get(f"{API_BASE}/models", timeout=5)
        if r.ok:
            data = (r.json() or {}).get("data") or {}
            models = [{"id": m.get("id"), "name": m.get("name") or m.get("id"),
                       "description": m.get("description") or ""}
                      for m in (data.get("models") or []) if m.get("id")]
            if models:
                payload = {"ok": True, "models": models, "current_model": data.get("current_model")}
    except (requests.exceptions.RequestException, ValueError):
        pass
    with _MODELS_LOCK:
        _MODELS_CACHE["at"] = time.monotonic()
        _MODELS_CACHE["payload"] = payload
    return jsonify(payload)


def _sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _prepare_turn(data: dict):
    """Shared parse + health gate + prompt build for chat routes.

    Returns (message, model, attachments, search_q, search_block, notices,
             sid, prompt, meta) or (None, log, health) on early error.
    """
    message = str(data.get("message") or "").strip()
    model = str(data.get("model") or "").strip() or None
    search_q = str(data.get("search") or "").strip()
    search_block = str(data.get("search_block") or "")
    attachments = data.get("attachments")
    if not isinstance(attachments, list):
        attachments = []
    edit_last = bool(data.get("edit_last"))

    if not message:
        return None, f"🛑 [LOG] {_ts()} | Empty message — nothing to send.", None

    health = check_health()
    if health["state"] != "ok":
        log = _rate_log() if health["state"] == "limited" else _offline_log()
        return None, log, health

    notices: list[str] = []
    block = search_block or None
    if not block and search_q:
        block = cached_search(search_q)
        if not block:
            notices.append(f'🔍 [LOG] {_ts()} | Web search unavailable for "{search_q}" — sent without results.')

    popped_user = ""
    with _LOCK:
        sid = _ensure_session()
        msgs = _SESSIONS[sid]["messages"]
        if edit_last:
            if msgs and msgs[-1].get("role") == "assistant":
                msgs.pop()
            if msgs and msgs[-1].get("role") == "user":
                popped_user = str(msgs[-1].get("content") or "")[:60]
                msgs.pop()
        context = list(msgs)

    prompt, cnotices = compose_prompt(context, message, attachments, block)
    notices.extend(cnotices)
    meta = {"search": search_q, "search_block": block or "", "attachments": attachments}

    with _LOCK:
        entry = _SESSIONS.get(sid)
        if entry is not None:
            entry["messages"].append({"role": "user", "content": message,
                                      "meta": meta, "ts": _ts()})
            # Keep the auto-title meaningful after an edit-resend.
            if edit_last and entry.get("title") == popped_user:
                entry["title"] = message[:60]
            if not entry.get("title"):
                entry["title"] = message[:60]
    _save_session(sid)
    return (message, model, attachments, search_q, block, notices, sid, prompt, meta), None, health


def _commit_assistant(sid: str, reply: str, latency_ms: int, tokens=None, partial=False) -> None:
    with _LOCK:
        entry = _SESSIONS.get(sid)
        if entry is None:
            return
        entry["messages"].append({
            "role": "assistant", "content": reply, "ts": _ts(),
            "latency_ms": latency_ms, "tokens": tokens, "partial": bool(partial),
        })
        entry["messages"] = _trim(entry["messages"])
        entry["active"] = _ts()
    _save_session(sid)


@app.post("/api/chat/stream")
def api_chat_stream():
    data = request.get_json(silent=True) or {}

    def gen():
        prepared, log, health = _prepare_turn(data)
        if prepared is None:
            yield _sse("error", {"log": log, "health": health})
            return
        (_message, model, _att, _q, _block, notices, sid, prompt, _meta) = prepared

        started = time.monotonic()
        parts: list[str] = []
        upstream_meta: dict = {}
        error_log = None
        committed = False
        reply = ""

        try:
            for kind, payload in _upstream_stream(prompt, model):
                if time.monotonic() - started > CHAT_DEADLINE:
                    error_log = f"⏱️ [LOG] {_ts()} | Stream exceeded {int(CHAT_DEADLINE)}s — stopped."
                    break
                if kind == "delta":
                    if payload:
                        parts.append(payload)
                        yield _sse("delta", {"delta": payload})
                elif kind == "done":
                    upstream_meta = payload or {}
        except RuntimeError as exc:
            msg = str(exc)
            if msg == "rate":
                error_log = _rate_log()
            elif msg.startswith("http:"):
                error_log = f"🛑 [LOG] {_ts()} | Upstream HTTP {msg.split(':', 2)[1]} — {msg.split(':', 2)[2][:140]}"
            else:
                error_log = f"🛑 [LOG] {_ts()} | Upstream stream error: {msg[:140]}"
        except requests.exceptions.Timeout:
            error_log = f"⏱️ [LOG] {_ts()} | Upstream stream timed out after {int(CHAT_TIMEOUT)}s."
        except requests.exceptions.ConnectionError:
            error_log = _offline_log()
        except requests.exceptions.RequestException as exc:
            error_log = f"🛑 [LOG] {_ts()} | Upstream stream failed: {exc}"
        finally:
            reply = "".join(parts).strip()
            if reply and not committed:
                latency = int((time.monotonic() - started) * 1000)
                _commit_assistant(sid, reply, latency,
                                  tokens=upstream_meta.get("tokens_estimate"),
                                  partial=bool(error_log))
                committed = True

        if not reply:
            yield _sse("error", {"log": error_log or f"🛑 [LOG] {_ts()} | Empty reply from upstream.",
                                 "health": _failure_health("rate" if error_log and "429" in error_log else "error"),
                                 "session_id": sid, "notices": notices})
            return

        latency = int((time.monotonic() - started) * 1000)
        yield _sse("final", {
            "ok": True,
            "html": render_md(reply),
            "session_id": sid,
            "notices": notices,
            "latency_ms": latency,
            "chars": len(reply),
            "tokens": upstream_meta.get("tokens_estimate"),
            "partial": bool(error_log),
            "error_log": error_log,
            "health": health,
        })

    resp = Response(gen(), mimetype="text/event-stream")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp


@app.post("/api/chat")
def api_chat():
    data = request.get_json(silent=True) or {}
    prepared, log, health = _prepare_turn(data)
    if prepared is None:
        return jsonify({"ok": False, "log": log, "health": health})
    (_message, model, _att, _q, _block, notices, sid, prompt, _meta) = prepared

    kind, payload = _upstream_json(prompt, model)
    if kind == "ok":
        reply = payload
        _commit_assistant(sid, reply, latency_ms=0)
        return jsonify({"ok": True, "reply": reply, "reply_html": render_md(reply),
                        "session_id": sid, "notices": notices, "health": health})
    return jsonify({"ok": False, "log": payload, "health": _failure_health(kind),
                    "notices": notices, "session_id": sid})


@app.post("/api/retry")
def api_retry():
    data = request.get_json(silent=True) or {}
    model = str(data.get("model") or "").strip() or None

    health = check_health()
    if health["state"] != "ok":
        log = _rate_log() if health["state"] == "limited" else _offline_log()
        return jsonify({"ok": False, "log": log, "health": health})

    with _LOCK:
        sid = _ensure_session()
        msgs = _SESSIONS[sid]["messages"]
        if msgs and msgs[-1].get("role") == "assistant":
            msgs.pop()
        if not msgs or msgs[-1].get("role") != "user":
            return jsonify({"ok": False, "log": f"🔁 [LOG] {_ts()} | Nothing to regenerate."})
        last = msgs[-1]
        context = list(msgs[:-1])
        meta = last.get("meta") or {}
        new_content = str(last.get("content") or "")

    search_block = str(meta.get("search_block") or "") or None
    prompt, notices = compose_prompt(context, new_content, meta.get("attachments") or [], search_block)
    kind, payload = _upstream_json(prompt, model)

    if kind == "ok":
        reply = payload
        _commit_assistant(sid, reply, latency_ms=0)
        return jsonify({"ok": True, "reply": reply, "reply_html": render_md(reply),
                        "session_id": sid, "notices": notices, "health": health})
    return jsonify({"ok": False, "log": payload, "health": _failure_health(kind), "notices": notices})


@app.post("/api/session/new")
def api_session_new():
    global _CURRENT
    with _LOCK:
        _CURRENT = _new_session_id()
        _SESSIONS[_CURRENT] = {"messages": [], "created": _ts(), "active": _ts(), "title": ""}
        _ORDER.append(_CURRENT)
        _prune_sessions()
        new_id = _CURRENT
    _save_session(new_id)

    upstream = "ok"
    try:
        HTTP.delete(f"{API_BASE}/history", timeout=5)
    except requests.exceptions.RequestException:
        upstream = "unreachable"

    log = f"🔄 [LOG] {_ts()} | Session cleared. New ID: {new_id}. History reset. Ready."
    return jsonify({"ok": True, "session_id": new_id, "log": log, "upstream": upstream})


@app.get("/api/sessions")
def api_sessions():
    with _LOCK:
        items = []
        for sid in reversed(_ORDER):
            entry = _SESSIONS.get(sid)
            if not entry:
                continue
            items.append({
                "id": sid,
                "title": entry.get("title") or "",
                "turns": sum(1 for m in entry["messages"] if m.get("role") == "user"),
                "active": entry["active"],
                "current": sid == _CURRENT,
            })
            if len(items) >= 15:
                break
    return jsonify({"ok": True, "sessions": items})


@app.post("/api/session/load")
def api_session_load():
    global _CURRENT
    data = request.get_json(silent=True) or {}
    sid = str(data.get("session_id") or "")
    with _LOCK:
        entry = _SESSIONS.get(sid)
        if entry is None:
            return jsonify({"ok": False, "log": f"📂 [LOG] {_ts()} | Unknown session: {sid[:80]}"})
        _CURRENT = sid
        entry["active"] = _ts()
        msgs = [dict(m) for m in entry["messages"]]
    out = []
    for m in msgs:
        item = {"role": m.get("role"), "content": m.get("content") or "",
                "ts": m.get("ts"), "latency_ms": m.get("latency_ms"),
                "tokens": m.get("tokens"), "partial": m.get("partial")}
        if m.get("role") == "assistant":
            item["html"] = render_md(str(m.get("content") or ""))
        out.append(item)
    return jsonify({"ok": True, "session_id": sid, "messages": out,
                    "title": entry.get("title") or ""})


@app.post("/api/search")
def api_search():
    data = request.get_json(silent=True) or {}
    query = str(data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "log": f"🔍 [LOG] {_ts()} | No search query given."})
    block = cached_search(query)
    if not block:
        return jsonify({"ok": False,
                        "log": f'🔍 [LOG] {_ts()} | Web search failed for "{query}" (DuckDuckGo unreachable or blocked).'})
    return jsonify({"ok": True, "query": query, "block": block})


@app.get("/api/tree")
def api_tree():
    return jsonify({"ok": True, "path": str(WORKSPACE), "tree": build_tree(WORKSPACE)})


@app.post("/api/workspace")
def api_workspace():
    global WORKSPACE
    data = request.get_json(silent=True) or {}
    raw = str(data.get("path") or "").strip() or str(DEFAULT_WORKSPACE)
    try:
        wp = Path(raw).expanduser().resolve()
        wp.mkdir(parents=True, exist_ok=True)
    except (OSError, ValueError) as exc:
        return jsonify({"ok": False, "log": f"🗂 [LOG] {_ts()} | Cannot open workspace '{raw[:120]}': {exc}"})
    if not wp.is_dir():
        return jsonify({"ok": False, "log": f"🗂 [LOG] {_ts()} | Not a folder: {raw[:120]}"})
    WORKSPACE = wp
    return jsonify({"ok": True, "path": str(wp), "tree": build_tree(wp)})


@app.get("/api/read")
def api_read():
    """Preview a file. Big files truncate with a warning; binaries preview
    via latin-1 instead of being refused (personal tool, no restrictions)."""
    rel = (request.args.get("path") or "").strip()
    full = request.args.get("full") in ("1", "true", "yes")
    if not rel:
        return jsonify({"ok": False, "log": f"📄 [LOG] {_ts()} | No path given."})
    path = Path(rel).expanduser()
    if not path.is_absolute():
        path = WORKSPACE / path
    try:
        path = path.resolve()
    except OSError:
        return jsonify({"ok": False, "log": f"📄 [LOG] {_ts()} | Bad path: {rel[:120]}"})
    if path.is_dir():
        return jsonify({"ok": False, "log": f"📄 [LOG] {_ts()} | '{rel}' is a folder — pick a file."})
    if not path.is_file():
        return jsonify({"ok": False, "log": f"📄 [LOG] {_ts()} | File not found: {rel[:160]}"})
    try:
        size = path.stat().st_size
        limit = PREVIEW_HARD_CAP if full else MAX_FILE_READ
        with open(path, "rb") as fh:
            data = fh.read(limit)
    except OSError as exc:
        return jsonify({"ok": False, "log": f"📄 [LOG] {_ts()} | Cannot read {rel[:120]}: {exc}"})
    binary = b"\x00" in data[:4096]
    text = data.decode("latin-1" if binary else "utf-8", errors="replace")
    return jsonify({"ok": True, "path": str(path), "content": text,
                    "size": size, "truncated": size > limit, "binary": binary})


@app.post("/api/url")
def api_url():
    data = request.get_json(silent=True) or {}
    url = str(data.get("url") or "").strip()
    if not re.match(r"^https?://", url, re.I):
        return jsonify({"ok": False, "log": f"🔗 [LOG] {_ts()} | Invalid URL (need http/https): {url[:120]}"})
    try:
        r = HTTP.get(url, timeout=STREAM_CONNECT_TIMEOUT, headers={"User-Agent": UA})
    except requests.exceptions.Timeout:
        return jsonify({"ok": False, "log": f"🔗 [LOG] {_ts()} | URL timed out: {url[:120]}"})
    except requests.exceptions.RequestException as exc:
        return jsonify({"ok": False, "log": f"🔗 [LOG] {_ts()} | URL fetch failed: {exc}"})
    if r.status_code >= 400:
        return jsonify({"ok": False, "log": f"🔗 [LOG] {_ts()} | URL returned HTTP {r.status_code}: {url[:120]}"})
    text = _strip_html(r.text or "")[:5000]
    if not text:
        return jsonify({"ok": False, "log": f"🔗 [LOG] {_ts()} | No readable text at {url[:120]}"})
    name = re.sub(r"^https?://", "", url)[:70]
    return jsonify({"ok": True, "name": name, "content": text, "chars": len(text)})


@app.post("/api/save")
def api_save():
    """User-initiated save of a reply/chat into workspace/exports."""
    data = request.get_json(silent=True) or {}
    content = str(data.get("content") or "")
    if not content.strip():
        return jsonify({"ok": False, "log": f"💾 [LOG] {_ts()} | Nothing to save."})
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(data.get("name") or ""))[:80]
    if not name:
        name = f"ds3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    if not name.lower().endswith((".md", ".txt", ".json")):
        name += ".md"
    try:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        target = EXPORT_DIR / name
        target.write_text(content, encoding="utf-8")
    except OSError as exc:
        return jsonify({"ok": False, "log": f"💾 [LOG] {_ts()} | Save failed: {exc}"})
    return jsonify({"ok": True, "path": str(target), "name": name})


# =============================================================================
# Pygments stylesheet (injected into the page; empty when unavailable)
# =============================================================================

PYGMENTS_CSS = ""
if PYGMENTS:
    try:
        PYGMENTS_CSS = HtmlFormatter(style="monokai").get_style_defs(".hl")
    except Exception:
        PYGMENTS_CSS = ""


# =============================================================================
# Embedded UI
# =============================================================================

INDEX_HTML = r"""
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DS3 Studio</title>
<style>
:root{
  --bg:#111827; --panel:#1f2937; --panel2:#111827; --border:#374151;
  --text:#f3f4f6; --muted:#9ca3af; --accent:#3b82f6; --userbg:#1e40af;
  --codebg:#0f172a; --okbg:rgba(34,197,94,.14); --okfg:#4ade80; --okbd:#14532d;
  --warnbg:rgba(234,179,8,.14); --warnfg:#facc15; --warnbd:#713f12;
  --errbg:rgba(239,68,68,.14); --errfg:#f87171; --errbd:#7f1d1d;
}
[data-theme="light"]{
  --bg:#f1f5f9; --panel:#ffffff; --panel2:#f8fafc; --border:#d1d5db;
  --text:#111827; --muted:#6b7280; --accent:#2563eb; --userbg:#2563eb;
  --codebg:#f8fafc; --okbg:rgba(34,197,94,.16); --okfg:#15803d; --okbd:#86efac;
  --warnbg:rgba(234,179,8,.2); --warnfg:#a16207; --warnbd:#fde68a;
  --errbg:rgba(239,68,68,.16); --errfg:#b91c1c; --errbd:#fca5a5;
}
*{box-sizing:border-box}
html,body{height:100%}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
#app{display:flex;height:100vh;overflow:hidden}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:6px}

/* sidebar */
#sidebar{position:relative;flex:0 0 auto;width:300px;min-width:0;background:var(--panel);border-right:1px solid var(--border);overflow:hidden;transition:width .3s ease,transform .3s ease}
#sidebar.collapsed{width:46px}
#sidebar.dragging{transition:none}
#sideInner{width:100%;height:100%;overflow-y:auto;padding:14px 46px 24px 14px;transition:opacity .2s ease-in-out}
#sidebar.collapsed #sideInner{opacity:0;pointer-events:none}
.side-toggle{position:absolute;top:10px;right:6px;z-index:6;width:28px;height:28px;border-radius:8px;border:1px solid var(--border);background:var(--panel2);color:var(--text);cursor:pointer;font-size:14px;transition:all .2s ease-in-out}
.side-toggle:hover{background:var(--border)}
.side-resize{position:absolute;top:0;right:0;width:5px;height:100%;cursor:col-resize;z-index:5}
.side-resize:hover{background:var(--accent);opacity:.5}
#sidebar.collapsed .side-resize{display:none}
.side-title{font-size:16px;font-weight:700;margin-bottom:10px;padding-right:30px}
.side-row{display:flex;gap:6px;align-items:center;margin-bottom:10px}
.side-row .spacer{flex:1}
.badge{display:block;width:100%;text-align:center;padding:8px 6px;border-radius:8px;font-weight:600;font-size:12.5px;cursor:pointer;border:1px solid transparent;transition:all .2s ease-in-out;margin-bottom:14px;user-select:none}
.badge:hover{filter:brightness(1.12)}
.badge-ok{background:var(--okbg);color:var(--okfg);border-color:var(--okbd)}
.badge-limited{background:var(--warnbg);color:var(--warnfg);border-color:var(--warnbd)}
.badge-offline{background:var(--errbg);color:var(--errfg);border-color:var(--errbd)}
.badge-unknown{background:rgba(156,163,175,.14);color:var(--muted);border-color:var(--border)}
.panel{margin-bottom:16px}
.panel h3{font-size:10.5px;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);margin:0 0 7px}
.row{display:flex;gap:6px}
.input{width:100%;background:var(--panel2);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:7px 9px;font-size:13px;outline:none;transition:border-color .2s ease-in-out;min-width:0}
.input:focus{border-color:var(--accent)}
select.input{cursor:pointer}
.btn{background:var(--border);border:1px solid #4b5563;color:var(--text);border-radius:8px;padding:7px 12px;font-size:13px;cursor:pointer;transition:all .2s ease-in-out;white-space:nowrap}
.btn:hover{filter:brightness(1.1)}
.btn.primary{background:var(--accent);border-color:#2563eb;color:#fff}
.btn.danger{background:#7f1d1d;border-color:#991b1b;color:#fecaca}
.btn.small{padding:7px 10px;font-size:12.5px}
.btn.icon{width:30px;height:30px;padding:0;font-size:14px}
.btn:disabled{opacity:.55;cursor:not-allowed}
.tree{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12.5px;background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:8px;max-height:190px;overflow:auto;margin-top:8px}
.tree .node{padding:2px 4px;border-radius:5px;white-space:nowrap;transition:background .2s ease-in-out}
.tree .node.file{cursor:pointer}
.tree .node:hover{background:var(--border)}
.tree .dir{color:#93c5fd}
.preview{height:130px;overflow:auto;background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px;white-space:pre-wrap;word-break:break-word;color:var(--text);margin:8px 0 0;font-family:ui-monospace,Menlo,Consolas,monospace}
.sessions .session-item{padding:6px 8px;border-radius:6px;cursor:pointer;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:var(--text);transition:background .2s ease-in-out;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sessions .session-item:hover{background:var(--border)}
.sessions .session-item.current{background:rgba(59,130,246,.18)}
.muted{color:var(--muted);font-size:12px}

/* main */
main{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;height:100vh}
#topbar{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 16px;background:var(--panel);border-bottom:1px solid var(--border)}
#sidLabel{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:46vw}
.top-actions{display:flex;gap:8px;flex-shrink:0}
#chat{position:relative;flex:1 1 auto;overflow-y:auto;padding:20px 18px 10px}
#emptyState{max-width:640px;margin:9vh auto 0;text-align:center;color:var(--muted)}
#emptyState h1{color:var(--text);font-size:26px;margin:0 0 6px}
#emptyState .tips{font-size:13px;margin-top:10px}
.quick{display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-top:16px}
kbd{background:var(--border);border-radius:5px;padding:1px 6px;font-size:11.5px;border:1px solid var(--border)}
#jumpBtn{position:absolute;right:22px;bottom:14px;display:none;background:var(--accent);color:#fff;border:none;border-radius:999px;padding:7px 14px;font-size:12.5px;cursor:pointer;box-shadow:0 6px 18px rgba(0,0,0,.35);transition:all .2s ease-in-out;z-index:20}
#jumpBtn.show{display:block}

/* messages */
.msg{position:relative;max-width:min(800px,92%);margin:0 0 14px;padding:11px 15px;border-radius:12px;transition:all .2s ease-in-out}
.msg.user{margin-left:auto;background:var(--userbg);color:#eff6ff;border-bottom-right-radius:4px}
.msg.assistant{background:var(--panel);border:1px solid var(--border);border-bottom-left-radius:4px}
.msg.sys{max-width:100%;background:transparent;border:1px dashed var(--border);color:var(--muted);font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12.5px;white-space:pre-wrap;word-break:break-word;padding:9px 13px}
.msg .body{overflow-wrap:anywhere}
.msg .body p{margin:.35em 0}
.msg .body p:first-child{margin-top:0}
.msg .body p:last-child{margin-bottom:0}
.msg .body h1,.msg .body h2,.msg .body h3{margin:.6em 0 .35em;font-size:1.05em}
.msg .body ul,.msg .body ol{margin:.35em 0;padding-left:1.35em}
.msg .body a{color:#93c5fd}
.msg .body blockquote{margin:.5em 0;padding:2px 10px;border-left:3px solid var(--border);color:var(--muted)}
.msg .body table{border-collapse:collapse;margin:.5em 0}
.msg .body th,.msg .body td{border:1px solid var(--border);padding:5px 9px;font-size:13px}
.msg .body th{background:var(--panel2)}
.msg .body hr{border:none;border-top:1px solid var(--border);margin:.8em 0}
.msg .body pre{background:var(--codebg);border:1px solid var(--border);border-radius:8px;padding:10px 12px;overflow-x:auto;margin:.6em 0;max-height:420px;overflow-y:auto}
.msg .body code{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px}
.msg .body :not(pre)>code{background:var(--border);padding:1.5px 5px;border-radius:5px;font-size:12.5px}
.code-wrap{position:relative}
.code-copy{position:absolute;top:7px;right:7px;opacity:0;background:var(--border);border:1px solid #4b5563;color:var(--text);border-radius:6px;padding:3px 8px;font-size:11.5px;cursor:pointer;transition:all .2s ease-in-out}
.code-wrap:hover .code-copy{opacity:1}
.msg .actions{position:absolute;top:7px;right:9px;display:flex;gap:6px;opacity:0;transition:all .2s ease-in-out}
.msg:hover .actions{opacity:1}
.act{width:27px;height:27px;border-radius:7px;border:1px solid var(--border);background:var(--panel2);color:var(--muted);font-size:13px;cursor:pointer;transition:all .2s ease-in-out;line-height:1}
.act:hover{background:var(--border);color:var(--text)}
.msg .meta{font-size:11px;color:var(--muted);margin-top:7px;opacity:.45;transition:opacity .2s ease-in-out;font-family:ui-monospace,Menlo,Consolas,monospace}
.msg:hover .meta{opacity:.9}
.msg.streaming .body{white-space:pre-wrap}
.dots{display:inline-flex;gap:4px}
.dots i{width:7px;height:7px;border-radius:50%;background:var(--muted);animation:tdot 1.2s infinite ease-in-out}
.dots i:nth-child(2){animation-delay:.2s}
.dots i:nth-child(3){animation-delay:.4s}
@keyframes tdot{0%,80%,100%{opacity:.25;transform:scale(.85)}40%{opacity:1;transform:scale(1)}}
.tip{position:absolute;top:-27px;right:0;background:#14532d;color:#bbf7d0;font-size:11.5px;padding:3px 8px;border-radius:6px;white-space:nowrap;pointer-events:none;animation:tipfade 1.4s ease-in-out}
@keyframes tipfade{0%{opacity:0;transform:translateY(4px)}15%{opacity:1;transform:translateY(0)}85%{opacity:1}100%{opacity:0}}

/* composer */
#composer{position:relative;background:var(--panel);border-top:1px solid var(--border);padding:10px 14px 12px}
#composer.dragover{outline:2px dashed var(--accent);outline-offset:-6px;border-radius:12px}
#chips{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;align-items:center}
#chips:empty{display:none}
.chip{display:inline-flex;align-items:center;gap:7px;background:var(--border);border:1px solid #4b5563;border-radius:999px;padding:3px 8px 3px 11px;font-size:12px;color:var(--text);max-width:280px}
.chip .cn{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px}
.chip button{background:none;border:none;color:var(--muted);cursor:pointer;font-size:12px;padding:0 4px}
.chip button:hover{color:#f87171}
#gauge{margin-left:auto;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;color:var(--muted);border:1px solid var(--border);border-radius:999px;padding:2px 9px;transition:all .2s ease-in-out}
#gauge.near{color:var(--warnfg);border-color:var(--warnbd);background:var(--warnbg)}
#gauge.over{color:var(--errfg);border-color:var(--errbd);background:var(--errbg)}
.composer-row{display:flex;gap:8px;align-items:flex-end}
.btn.plus{width:42px;height:42px;font-size:20px;padding:0;border-radius:11px;font-weight:700}
#input{flex:1;resize:none;min-height:42px;max-height:220px;background:var(--panel2);border:1px solid var(--border);color:var(--text);border-radius:11px;padding:11px 13px;font:inherit;line-height:1.45;outline:none;transition:border-color .2s ease-in-out}
#input:focus{border-color:var(--accent)}
#input::placeholder{color:var(--muted)}
#sendBtn{height:42px;padding:0 20px;border-radius:11px;font-weight:600}
#sendBtn.stop{background:#b91c1c;border-color:#991b1b}

/* + slide-up menu */
#ctxMenu{position:absolute;left:14px;bottom:100%;margin-bottom:10px;min-width:350px;background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:9px;box-shadow:0 -10px 30px rgba(0,0,0,.45);transform:translateY(100%);opacity:0;pointer-events:none;transition:all .2s ease-in-out;z-index:40}
#ctxMenu.open{transform:translateY(0);opacity:1;pointer-events:auto}
.ctx-row{display:flex;gap:7px;align-items:center;padding:4px 3px}
.ctx-row .lbl{width:86px;flex-shrink:0;color:var(--muted);font-size:12.5px}
.ctx-row .input{flex:1}

/* toasts */
#toasts{position:fixed;right:18px;bottom:18px;display:flex;flex-direction:column;gap:8px;z-index:60;pointer-events:none}
.toast{background:var(--panel);border:1px solid var(--border);color:var(--text);border-radius:9px;padding:9px 14px;font-size:12.5px;box-shadow:0 8px 22px rgba(0,0,0,.35);animation:toastin .2s ease-in-out}
@keyframes toastin{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}

/* pygments */
/*PYGMENTS_CSS*/
</style>
</head>
<body>
<div id="app">
  <aside id="sidebar">
    <div id="sideInner">
      <div class="side-title">DS3 Studio</div>
      <div id="healthBadge" class="badge badge-unknown" title="Click to re-check the CLI">⚪ Checking…</div>

      <section class="panel">
        <h3>Model · /model</h3>
        <div class="side-row">
          <select id="modelSelect" class="input"></select>
          <button id="themeBtn" class="btn icon" title="Toggle theme">🌙</button>
        </div>
      </section>

      <section class="panel">
        <h3>Workspace</h3>
        <div class="row">
          <input id="wsPath" class="input" placeholder="workspace" spellcheck="false">
          <button id="wsApply" class="btn small">Apply</button>
        </div>
        <div id="tree" class="tree"><span class="muted">loading…</span></div>
      </section>

      <section class="panel">
        <h3>File preview · /file</h3>
        <div class="row">
          <input id="previewPath" class="input" placeholder="relative/path.txt" spellcheck="false">
          <button id="previewRead" class="btn small">Read</button>
        </div>
        <pre id="previewBox" class="preview">—</pre>
      </section>

      <section class="panel">
        <h3>Sessions · /history</h3>
        <div id="sessionList" class="sessions"><span class="muted">none yet</span></div>
      </section>
    </div>
    <div id="sideResize" class="side-resize" title="Drag to resize"></div>
    <button id="sideToggle" class="side-toggle" title="Collapse / expand sidebar">«</button>
  </aside>

  <main>
    <header id="topbar">
      <span id="sidLabel">session: —</span>
      <div class="top-actions">
        <button id="exportBtn" class="btn small">⬇ Export Chat</button>
        <button id="newChatBtn" class="btn small primary">🔄 Clear &amp; New Session</button>
      </div>
    </header>

    <div id="chat">
      <div id="emptyState">
        <h1>DS3 Studio</h1>
        <p>Streaming local UI for the Duck.ai Chat CLI.</p>
        <p class="tips"><kbd>Enter</kbd> send · <kbd>Shift</kbd>+<kbd>Enter</kbd> newline · <kbd>Esc</kbd> stop</p>
        <p class="tips">Replies stream in, render as markdown — hover for Copy / Save / Regenerate.</p>
        <div class="quick">
          <button class="btn small" data-quick="file">📎 Attach a file</button>
          <button class="btn small" data-quick="search">🔍 Search the web</button>
          <button class="btn small" data-quick="menu">＋ Commands</button>
        </div>
      </div>
      <div id="stream"></div>
      <button id="jumpBtn">↓ New messages</button>
    </div>

    <footer id="composer">
      <div id="ctxMenu">
        <div class="ctx-row"><span class="lbl">🤖 /model</span><select id="ctxModel" class="input"></select></div>
        <div class="ctx-row"><span class="lbl">📁 /file</span><input id="ctxFile" class="input" placeholder="path/to/file" spellcheck="false"><button class="btn small" data-ctx="file">Add</button></div>
        <div class="ctx-row"><span class="lbl">🔍 /search</span><input id="ctxSearch" class="input" placeholder="query"><button class="btn small" data-ctx="search">Add</button></div>
        <div class="ctx-row"><span class="lbl">🔗 /url</span><input id="ctxUrl" class="input" placeholder="https://…"><button class="btn small" data-ctx="url">Add</button></div>
        <div class="ctx-row"><span class="lbl">🧹 /clear</span><button class="btn small danger" data-ctx="clear">Clear &amp; New Session</button></div>
      </div>
      <div id="chips"><span id="gauge">0 / 9.9k</span></div>
      <div class="composer-row">
        <input type="file" id="filePick" multiple hidden>
        <button id="clipBtn" class="btn plus" title="Attach files from disk">📎</button>
        <button id="plusBtn" class="btn plus" title="CLI commands">＋</button>
        <textarea id="input" rows="1" placeholder="Message DS3…   (/model /file /url /search /clear /retry /copy /export)" spellcheck="false"></textarea>
        <button id="sendBtn" class="btn primary" disabled>Send</button>
      </div>
    </footer>
  </main>
</div>
<div id="toasts"></div>
<script>
'use strict';
const LS = { theme: 'ds3.theme', width: 'ds3.sideW', collapsed: 'ds3.collapsed', model: 'ds3.model' };
const $ = (id) => document.getElementById(id);
const S = {
  messages: [], els: [], session: null, model: null,
  healthState: 'unknown', busy: false, streaming: false,
  pending: { files: [], search: null, block: '' },
  abort: null, pinned: true, editing: false
};

/* ---------- utilities ---------- */
function TS(){ const d = new Date(); const p = (n) => String(n).padStart(2, '0');
  return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + ' ' +
         p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds()); }
function esc(t){ return String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;'); }
const rateLog = () => '⚠️ [LOG] ' + TS() + ' | Upstream 429/Rate Limit detected. The CLI is still in cooldown. Please wait or run /clear in the main CLI terminal.';
const offlineLog = () => '🔴 [LOG] ' + TS() + ' | CLI Offline: cannot reach http://127.0.0.1:8989. Start the Duck CLI, then click the badge to retry.';
function fmtSize(b){ if (b < 1024) return b + ' B'; if (b < 1048576) return (b / 1024).toFixed(1) + ' KB'; return (b / 1048576).toFixed(1) + ' MB'; }
function fmtK(n){ return n >= 1000 ? (n / 1000).toFixed(1) + 'k' : String(n); }

async function fetchJSON(url, opts){
  const o = Object.assign({ headers: { 'Content-Type': 'application/json' } }, opts || {});
  const r = await fetch(url, o);
  return r.json();
}
function download(name, text){
  const blob = new Blob([text], { type: 'text/markdown;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 3000);
}
async function copyText(t){
  try { await navigator.clipboard.writeText(t); return true; } catch (e) {}
  try {
    const ta = document.createElement('textarea');
    ta.value = t; ta.style.position = 'fixed'; ta.style.opacity = '0';
    document.body.appendChild(ta); ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return ok;
  } catch (e) { return false; }
}
function toast(text){
  const box = $('toasts');
  const el = document.createElement('div');
  el.className = 'toast';
  el.textContent = text;
  box.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity .3s'; }, 1900);
  setTimeout(() => el.remove(), 2300);
}
function showTip(anchor, text){
  const tip = document.createElement('span');
  tip.className = 'tip';
  tip.textContent = text;
  anchor.parentElement.appendChild(tip);
  setTimeout(() => tip.remove(), 1400);
}
function autosize(){
  const ta = $('input');
  ta.style.height = 'auto';
  ta.style.height = Math.min(ta.scrollHeight, 220) + 'px';
}

/* ---------- scroll pinning ---------- */
function chatEl(){ return $('chat'); }
function atBottom(){
  const c = chatEl();
  return c.scrollHeight - c.scrollTop - c.clientHeight < 56;
}
function scrollBottom(){ const c = chatEl(); c.scrollTop = c.scrollHeight; $('jumpBtn').classList.remove('show'); }
function maybeScroll(){ if (S.pinned) scrollBottom(); else $('jumpBtn').classList.add('show'); }
function updatePin(){ S.pinned = atBottom(); if (S.pinned) $('jumpBtn').classList.remove('show'); }

/* ---------- rendering ---------- */
function metaLine(m){
  if (m.role !== 'assistant') return '';
  const bits = [];
  if (m.latency_ms != null) bits.push((m.latency_ms / 1000).toFixed(1) + 's');
  if (m.tokens != null) bits.push('~' + m.tokens + ' tok');
  bits.push(String(m.content || '').length + ' chars');
  if (m.ts) bits.push(m.ts.split(' ')[1] || m.ts);
  if (m.partial) bits.push('partial');
  return bits.join(' · ');
}

function makeBubbleEl(m, idx){
  const el = document.createElement('div');
  el.dataset.idx = String(idx);
  if (m.role === 'system'){
    el.className = 'msg sys';
    el.textContent = m.content;
    return el;
  }
  el.className = 'msg ' + m.role;
  const body = document.createElement('div');
  body.className = 'body';
  if (m.role === 'user'){
    body.textContent = m.content;
  } else {
    body.innerHTML = m.html || ('<pre>' + esc(m.content) + '</pre>');
  }
  el.appendChild(body);
  if (m.role === 'assistant' && m.meta !== false){
    const meta = document.createElement('div');
    meta.className = 'meta';
    meta.textContent = metaLine(m);
    el.appendChild(meta);
  }
  return el;
}

function actionsHTML(m, idx, last){
  let html = '';
  if (m.role === 'assistant'){
    html += '<button class="act" data-a="copy" title="Copy">⧉</button>';
    html += '<button class="act" data-a="save" title="Download as .md">⬇</button>';
    html += '<button class="act" data-a="savews" title="Save to workspace/exports">💾</button>';
  }
  if (m.role === 'user' && last) html += '<button class="act" data-a="edit" title="Edit & resend">✎</button>';
  if (last) html += '<button class="act" data-a="retry" title="Regenerate">↻</button>';
  return html;
}

function refreshActions(){
  S.els.forEach((el, idx) => {
    const m = S.messages[idx];
    if (!m || m.role === 'system') return;
    const old = el.querySelector('.actions');
    if (old) old.remove();
    const last = idx === S.messages.length - 1;
    const html = actionsHTML(m, idx, last);
    if (!html) return;
    const act = document.createElement('div');
    act.className = 'actions';
    act.innerHTML = html;
    el.appendChild(act);
  });
}

function renderAll(){
  const stream = $('stream');
  stream.innerHTML = '';
  S.els = [];
  $('emptyState').style.display = S.messages.length ? 'none' : 'block';
  S.messages.forEach((m, i) => {
    const el = makeBubbleEl(m, i);
    stream.appendChild(el);
    S.els.push(el);
  });
  S.els.forEach((el) => enhanceCodeBlocks(el));
  refreshActions();
  updateGauge();
  $('sidLabel').textContent = 'session: ' + (S.session || '—');
  scrollBottom();
}

function appendBubble(m){
  const idx = S.messages.length;
  S.messages.push(m);
  const el = makeBubbleEl(m, idx);
  $('stream').appendChild(el);
  S.els.push(el);
  enhanceCodeBlocks(el);
  refreshActions();
  updateGauge();
  maybeScroll();
  return el;
}
function addSystem(text){ appendBubble({ role: 'system', content: text }); }

function enhanceCodeBlocks(root){
  root.querySelectorAll('pre').forEach(pre => {
    if (pre.parentElement && pre.parentElement.classList.contains('code-wrap')) return;
    const wrap = document.createElement('div');
    wrap.className = 'code-wrap';
    pre.parentElement.insertBefore(wrap, pre);
    wrap.appendChild(pre);
    const btn = document.createElement('button');
    btn.className = 'code-copy';
    btn.textContent = '⧉ Copy';
    btn.onclick = async () => {
      const ok = await copyText(pre.innerText);
      showTip(btn, ok ? '✅ Copied' : '❌ Blocked');
      if (ok) toast('Code copied');
    };
    wrap.appendChild(btn);
  });
}

/* ---------- context gauge ---------- */
function updateGauge(){
  let n = 0;
  S.messages.forEach(m => { n += String(m.content || '').length; });
  S.pending.files.forEach(f => { n += String(f.content || '').length; });
  if (S.pending.block) n += S.pending.block.length;
  const cap = 9900;
  const g = $('gauge');
  g.textContent = fmtK(n) + ' / ' + fmtK(cap);
  g.classList.toggle('near', n > cap * 0.75 && n <= cap);
  g.classList.toggle('over', n > cap);
}

/* ---------- health ---------- */
function applyHealth(h){
  S.healthState = (h && h.state) || 'unknown';
  const b = $('healthBadge');
  b.textContent = (h && h.badge) || '⚪ Unknown';
  b.className = 'badge badge-' + S.healthState;
  b.title = (((h && h.detail) || '') + ' — click to re-check').trim();
  refreshSend();
}
function refreshSend(){
  const btn = $('sendBtn');
  if (S.busy){
    btn.disabled = false;
    btn.textContent = '⏹ Stop';
    btn.classList.add('stop');
  } else {
    btn.disabled = S.healthState !== 'ok';
    btn.textContent = 'Send';
    btn.classList.remove('stop');
  }
}
async function pingHealth(force){
  if (!force) $('healthBadge').textContent = '⚪ Checking…';
  try { applyHealth(await fetchJSON('/api/health' + (force ? '?force=1' : ''))); }
  catch (e) { applyHealth({ state: 'offline', badge: '🔴 CLI Offline', send: false, detail: 'DS3 server unreachable' }); }
}

/* ---------- streaming send ---------- */
function parseEvent(raw){
  let name = 'message', data = null;
  raw.split('\n').forEach(line => {
    if (line.startsWith('event:')) name = line.slice(6).trim();
    else if (line.startsWith('data:')){
      try { data = JSON.parse(line.slice(5).trim()); } catch (e) {}
    }
  });
  return data ? { name: name, data: data } : null;
}

function streamBubbleEl(){
  const el = document.createElement('div');
  el.className = 'msg assistant streaming';
  const body = document.createElement('div');
  body.className = 'body';
  body.innerHTML = '<span class="dots"><i></i><i></i><i></i></span>';
  el.appendChild(body);
  $('stream').appendChild(el);
  maybeScroll();
  return el;
}

async function streamChat(payload){
  S.busy = true; S.streaming = true; S.abort = new AbortController();
  refreshSend();
  const el = streamBubbleEl();
  let acc = '';
  let finished = false;

  try {
    const res = await fetch('/api/chat/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: S.abort.signal
    });
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    while (true){
      const chunk = await reader.read();
      if (chunk.done) break;
      buf += dec.decode(chunk.value, { stream: true });
      let cut;
      while ((cut = buf.indexOf('\n\n')) >= 0){
        const raw = buf.slice(0, cut);
        buf = buf.slice(cut + 2);
        const evt = parseEvent(raw);
        if (!evt) continue;
        if (evt.name === 'delta'){
          acc += (evt.data.delta || '');
          el.querySelector('.body').textContent = acc;
          maybeScroll();
        } else if (evt.name === 'final'){
          finished = true;
          finalizeStream(el, acc, evt.data);
        } else if (evt.name === 'error'){
          if (evt.data.health) applyHealth(evt.data.health);
          addSystem(evt.data.log || ('🛑 [LOG] ' + TS() + ' | Stream error.'));
          (evt.data.notices || []).forEach(addSystem);
          el.remove();
          finished = true;
        }
      }
    }
  } catch (e) {
    if (e && e.name === 'AbortError'){
      if (acc.trim()) sealPartial(el, acc, '⏹ stopped');
      else el.remove();
      addSystem('⏹ [LOG] ' + TS() + ' | Stream stopped by user.');
    } else {
      if (acc.trim()) sealPartial(el, acc, '⚠️ connection lost');
      else el.remove();
      addSystem('🛑 [LOG] ' + TS() + ' | DS3 stream failed: ' + e);
    }
  } finally {
    if (!finished && acc.trim()) sealPartial(el, acc, 'partial');
    S.busy = false; S.streaming = false; S.abort = null;
    refreshSend();
    if (!finished && !acc.trim()) { /* bubble already removed */ }
    refreshSessions();
  }
}

function finalizeStream(el, acc, data){
  el.classList.remove('streaming');
  const body = el.querySelector('.body');
  body.innerHTML = data.html || ('<pre>' + esc(acc) + '</pre>');
  el.querySelectorAll('.meta').forEach(x => x.remove());
  const m = {
    role: 'assistant', content: acc, html: data.html || '',
    ts: TS(), latency_ms: data.latency_ms, tokens: data.tokens, partial: !!data.partial
  };
  if (data.error_log) addSystem(data.error_log);
  (data.notices || []).forEach(addSystem);
  if (data.health) applyHealth(data.health);
  if (data.session_id) S.session = data.session_id;
  const idx = S.messages.length;
  S.messages.push(m);
  el.dataset.idx = String(idx);
  S.els.push(el);
  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = metaLine(m);
  el.appendChild(meta);
  enhanceCodeBlocks(el);
  refreshActions();
  updateGauge();
}

function sealPartial(el, acc, label){
  el.classList.remove('streaming');
  el.querySelector('.body').textContent = acc;
  const m = { role: 'assistant', content: acc, ts: TS(), partial: true };
  const idx = S.messages.length;
  S.messages.push(m);
  el.dataset.idx = String(idx);
  S.els.push(el);
  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = label + ' · ' + acc.length + ' chars';
  el.appendChild(meta);
  refreshActions();
  updateGauge();
}

async function stopStream(){
  if (S.abort){ try { S.abort.abort(); } catch (e) {} }
}

/* ---------- send ---------- */
async function send(){
  if (S.busy){ await stopStream(); return; }
  const ta = $('input');
  const raw = ta.value.trim();
  if (!raw) return;
  if (await handleCommand(raw)){ ta.value = ''; autosize(); return; }

  let h = null;
  try { h = await fetchJSON('/api/health'); } catch (e) {}
  if (!h){ addSystem(offlineLog()); return; }
  applyHealth(h);
  if (h.state !== 'ok'){ addSystem(h.state === 'limited' ? rateLog() : offlineLog()); return; }

  const payload = {
    message: raw, model: S.model || null,
    search: S.pending.search || '',
    search_block: S.pending.block || '',
    attachments: S.pending.files.slice(),
    edit_last: S.editing
  };
  clearPending();
  ta.value = ''; autosize();

  if (S.editing){
    if (S.messages.length && S.messages[S.messages.length - 1].role === 'assistant') S.messages.pop();
    if (S.messages.length && S.messages[S.messages.length - 1].role === 'user') S.messages.pop();
    S.editing = false;
    renderAll();
  }
  appendBubble({ role: 'user', content: raw });
  await streamChat(payload);
}

async function retryLast(){
  if (S.busy) return;
  const last = S.messages[S.messages.length - 1];
  if (!last || (last.role !== 'assistant' && last.role !== 'user')){
    addSystem('🔁 [LOG] ' + TS() + ' | Nothing to regenerate.');
    return;
  }
  let h = null;
  try { h = await fetchJSON('/api/health'); } catch (e) {}
  if (!h){ addSystem(offlineLog()); return; }
  applyHealth(h);
  if (h.state !== 'ok'){ addSystem(h.state === 'limited' ? rateLog() : offlineLog()); return; }
  if (last.role === 'assistant') S.messages.pop();
  renderAll();
  S.busy = true; refreshSend();
  const el = streamBubbleEl();
  try {
    const res = await fetchJSON('/api/retry', { method: 'POST', body: JSON.stringify({ model: S.model || null }) });
    if (res.health) applyHealth(res.health);
    el.remove();
    if (res.ok){
      const m = { role: 'assistant', content: res.reply, html: res.reply_html, ts: TS() };
      appendBubble(m);
      (res.notices || []).forEach(addSystem);
      if (res.session_id) S.session = res.session_id;
    } else addSystem(res.log || ('🛑 [LOG] ' + TS() + ' | Retry failed.'));
  } catch (e) {
    el.remove();
    addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e);
  } finally {
    S.busy = false; refreshSend(); refreshSessions();
  }
}

/* ---------- slash commands ---------- */
async function handleCommand(raw){
  const m = raw.match(/^\/([A-Za-z]+)(?:[ \t]+([\s\S]+))?$/);
  if (!m) return false;
  const cmd = m[1].toLowerCase();
  const arg = (m[2] || '').trim();
  switch (cmd){
    case 'model': {
      const sel = $('modelSelect');
      if (arg){
        if (![...sel.options].some(o => o.value === arg)){
          const o = document.createElement('option');
          o.value = arg; o.textContent = arg;
          sel.appendChild(o);
        }
        sel.value = arg; S.model = arg; syncCtxModel();
        localStorage.setItem(LS.model, arg);
        addSystem('🤖 [LOG] ' + TS() + ' | Model set to ' + arg + '.');
        toast('Model → ' + arg);
      } else {
        expandSidebar(); sel.focus();
        addSystem('🤖 [LOG] ' + TS() + ' | Choose a model from the dropdown (top of the sidebar).');
      }
      return true;
    }
    case 'file': {
      if (!arg){ addSystem('📁 [LOG] ' + TS() + ' | Usage: /file <path> — workspace-relative or absolute.'); return true; }
      try {
        const res = await fetchJSON('/api/read?path=' + encodeURIComponent(arg));
        if (res.ok){
          const name = String(res.path || arg).split(/[\\/]/).pop();
          addPending(name, res.content);
          addSystem('📎 [LOG] ' + TS() + ' | Attached file: ' + name + ' (' + res.content.length + ' chars' + (res.truncated ? ', truncated' : '') + ').');
          toast('Attached ' + name);
        } else addSystem(res.log || ('📁 [LOG] ' + TS() + ' | Cannot read ' + arg + '.'));
      } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
      return true;
    }
    case 'url': {
      if (!arg || !/^https?:\/\//i.test(arg)){ addSystem('🔗 [LOG] ' + TS() + ' | Usage: /url https://example.com/page'); return true; }
      try {
        const res = await fetchJSON('/api/url', { method: 'POST', body: JSON.stringify({ url: arg }) });
        if (res.ok){
          addPending(res.name, res.content);
          addSystem('🔗 [LOG] ' + TS() + ' | Attached URL: ' + res.name + ' (' + res.content.length + ' chars).');
          toast('Attached ' + res.name);
        } else addSystem(res.log || ('🔗 [LOG] ' + TS() + ' | Cannot fetch ' + arg + '.'));
      } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
      return true;
    }
    case 'search': {
      if (!arg){ S.pending.search = null; S.pending.block = ''; renderChips(); addSystem('🔍 [LOG] ' + TS() + ' | Web search cleared.'); return true; }
      S.pending.search = arg; S.pending.block = ''; renderChips();
      addSystem('🔍 [LOG] ' + TS() + ' | Web search armed for the next message: ' + arg);
      prefetchSearch(arg);
      return true;
    }
    case 'clear': await clearSession(); return true;
    case 'retry': await retryLast(); return true;
    case 'copy': {
      const i = lastAssistantIdx();
      if (i < 0){ addSystem('⧉ [LOG] ' + TS() + ' | Nothing to copy yet.'); return true; }
      const ok = await copyText(S.messages[i].content);
      addSystem(ok ? ('✅ [LOG] ' + TS() + ' | Last reply copied to the clipboard.')
                   : ('🛑 [LOG] ' + TS() + ' | Clipboard blocked by the browser.'));
      if (ok) toast('Copied');
      return true;
    }
    case 'export': exportChat(); return true;
    case 'history':
    case 'sessions': {
      expandSidebar();
      const el = $('sessionList');
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      return true;
    }
    default: return false;
  }
}

function lastAssistantIdx(){
  for (let i = S.messages.length - 1; i >= 0; i--){
    if (S.messages[i].role === 'assistant') return i;
  }
  return -1;
}

async function prefetchSearch(q){
  try {
    const res = await fetchJSON('/api/search', { method: 'POST', body: JSON.stringify({ query: q }) });
    if (res.ok && S.pending.search === q){ S.pending.block = res.block; renderChips(); toast('Search results ready'); }
    else if (!res.ok && S.pending.search === q){ addSystem(res.log || ''); }
  } catch (e) {}
}

/* ---------- pending chips ---------- */
function addPending(name, content){
  S.pending.files.push({ name: name, content: content });
  renderChips();
}
function renderChips(){
  const c = $('chips');
  const gauge = $('gauge');
  c.innerHTML = '';
  S.pending.files.forEach((f, i) => {
    const chip = document.createElement('span');
    chip.className = 'chip';
    chip.innerHTML = '📎 <span class="cn">' + esc(f.name) + '</span><button title="Remove">✕</button>';
    chip.querySelector('button').onclick = () => { S.pending.files.splice(i, 1); renderChips(); };
    c.appendChild(chip);
  });
  if (S.pending.search){
    const chip = document.createElement('span');
    chip.className = 'chip';
    const mark = S.pending.block ? ' ✅' : '';
    chip.innerHTML = '🔍 <span class="cn">' + esc(S.pending.search) + mark + '</span><button title="Remove">✕</button>';
    chip.querySelector('button').onclick = () => { S.pending.search = null; S.pending.block = ''; renderChips(); };
    c.appendChild(chip);
  }
  c.appendChild(gauge);
  updateGauge();
}
function clearPending(){ S.pending.files = []; S.pending.search = null; S.pending.block = ''; renderChips(); }

/* ---------- sessions ---------- */
async function clearSession(){
  if (S.busy) return;
  try {
    const res = await fetchJSON('/api/session/new', { method: 'POST', body: '{}' });
    if (res.ok){
      S.messages = []; S.els = []; S.session = res.session_id; S.editing = false;
      renderAll();
      if (res.log) addSystem(res.log);
      refreshSessions();
      toast('New session');
    } else addSystem(res.log || ('🛑 [LOG] ' + TS() + ' | Failed to clear the session.'));
  } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
}

async function refreshSessions(){
  try {
    const res = await fetchJSON('/api/sessions');
    const el = $('sessionList');
    el.innerHTML = '';
    const list = res.sessions || [];
    if (!list.length){ el.innerHTML = '<span class="muted">none yet</span>'; return; }
    list.forEach(s => {
      if (s.current) S.session = s.id;
      const d = document.createElement('div');
      d.className = 'session-item' + (s.current ? ' current' : '');
      d.textContent = (s.title ? s.title : s.id) + ' · ' + s.turns + 't';
      d.title = s.id + '\nlast active: ' + s.active + '\nclick to load';
      d.onclick = () => loadSession(s.id);
      el.appendChild(d);
    });
  } catch (e) {}
}

async function loadSession(id){
  if (S.busy) return;
  try {
    const res = await fetchJSON('/api/session/load', { method: 'POST', body: JSON.stringify({ session_id: id }) });
    if (res.ok){
      S.session = res.session_id;
      S.messages = (res.messages || []).map(m => ({
        role: m.role, content: m.content, html: m.html,
        ts: m.ts, latency_ms: m.latency_ms, tokens: m.tokens, partial: m.partial
      }));
      renderAll();
      addSystem('📂 [LOG] ' + TS() + ' | Loaded session ' + id + '.');
      refreshSessions();
    } else addSystem(res.log || ('📂 [LOG] ' + TS() + ' | Cannot load ' + id + '.'));
  } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
}

/* ---------- models ---------- */
async function loadModels(){
  try {
    const res = await fetchJSON('/api/models');
    const models = res.models || [];
    fillSelect($('modelSelect'), models);
    const saved = localStorage.getItem(LS.model);
    if (saved && models.some(m => m.id === saved)){ $('modelSelect').value = saved; S.model = saved; }
    else if (res.current_model){ $('modelSelect').value = res.current_model; S.model = res.current_model; }
    else if (models.length){ S.model = models[0].id; $('modelSelect').value = S.model; }
    syncCtxModel();
  } catch (e) {}
}
function fillSelect(sel, models){
  sel.innerHTML = '';
  models.forEach(m => {
    const o = document.createElement('option');
    o.value = m.id; o.textContent = m.name || m.id; o.title = m.description || '';
    sel.appendChild(o);
  });
}
function syncCtxModel(){
  const src = $('modelSelect');
  fillSelect($('ctxModel'), [...src.options].map(o => ({ id: o.value, name: o.textContent })));
  if (S.model) $('ctxModel').value = S.model;
}

/* ---------- workspace ---------- */
async function loadTree(){
  try {
    const res = await fetchJSON('/api/tree');
    if (res.ok){ $('wsPath').value = res.path; renderTree(res.tree, 0); }
  } catch (e) { $('tree').textContent = 'tree unavailable'; }
}
async function applyWorkspace(){
  const p = $('wsPath').value.trim();
  if (!p) return;
  try {
    const res = await fetchJSON('/api/workspace', { method: 'POST', body: JSON.stringify({ path: p }) });
    if (res.ok){
      $('wsPath').value = res.path;
      renderTree(res.tree, 0);
      addSystem('🗂 [LOG] ' + TS() + ' | Workspace → ' + res.path);
      toast('Workspace updated');
    } else addSystem(res.log || ('🗂 [LOG] ' + TS() + ' | Cannot open workspace.'));
  } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
}
function renderTree(nodes, depth){
  const t = $('tree');
  if (depth === 0) t.innerHTML = '';
  if (!nodes || !nodes.length){
    if (depth === 0) t.innerHTML = '<span class="muted">(empty folder)</span>';
    return;
  }
  nodes.forEach(n => {
    const d = document.createElement('div');
    d.className = 'node ' + (n.type === 'dir' ? 'dir' : 'file');
    d.style.paddingLeft = (depth * 14) + 'px';
    const sz = (n.type === 'file' && n.size != null) ? ' (' + fmtSize(n.size) + ')' : '';
    d.textContent = (n.type === 'dir' ? '📁 ' : '📄 ') + n.name + (n.type === 'dir' ? '/' : '') + sz;
    if (n.type === 'file'){
      d.title = 'Click to preview ' + (n.path || n.name);
      d.onclick = () => { $('previewPath').value = n.path || n.name; readPreview(); };
    }
    t.appendChild(d);
    if (n.type === 'dir' && n.children && n.children.length) renderTree(n.children, depth + 1);
  });
}
async function readPreview(full){
  const p = $('previewPath').value.trim();
  if (!p){ addSystem('📄 [LOG] ' + TS() + ' | Type a relative path, then click Read.'); return; }
  try {
    const res = await fetchJSON('/api/read?path=' + encodeURIComponent(p) + (full ? '&full=1' : ''));
    if (res.ok){
      const notes = [];
      if (res.truncated) notes.push('truncated to ' + fmtSize(MAX_PREVIEW));
      if (res.binary) notes.push('binary (latin-1)');
      $('previewBox').textContent = res.content;
      addSystem('📄 [LOG] ' + TS() + ' | Read ' + res.path + ' (' + fmtSize(res.size) + (notes.length ? ' · ' + notes.join(' · ') : '') + ').');
    } else {
      $('previewBox').textContent = '—';
      addSystem(res.log || ('📄 [LOG] ' + TS() + ' | Cannot read ' + p + '.'));
    }
  } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
}
const MAX_PREVIEW = 200000;

/* ---------- export / save ---------- */
function exportChat(){
  if (!S.messages.length){ addSystem('⬇ [LOG] ' + TS() + ' | Nothing to export yet.'); return; }
  let out = '# DS3 Chat Export\n\n- Session: ' + (S.session || '—') + '\n- Model: ' + (S.model || '—') +
            '\n- Exported: ' + TS() + '\n\n---\n\n';
  S.messages.forEach(m => {
    if (m.role === 'system') out += '> ' + String(m.content).replace(/\n/g, '\n> ') + '\n\n';
    else out += '**' + (m.role === 'user' ? 'User' : 'Assistant') + ':**\n\n' + m.content + '\n\n';
  });
  const safe = String(S.session || 'session').replace(/[^\w-]+/g, '_');
  download('ds3_chat_' + safe + '_' + Date.now() + '.md', out);
  addSystem('⬇ [LOG] ' + TS() + ' | Chat exported as .md.');
  toast('Chat exported');
}

async function saveToWorkspace(m){
  try {
    const res = await fetchJSON('/api/save', {
      method: 'POST',
      body: JSON.stringify({ name: 'reply_' + Date.now() + '.md', content: m.content })
    });
    if (res.ok){ addSystem('💾 [LOG] ' + TS() + ' | Saved to ' + res.path); toast('Saved to workspace'); loadTree(); }
    else addSystem(res.log || ('💾 [LOG] ' + TS() + ' | Save failed.'));
  } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | DS3 server unreachable: ' + e); }
}

/* ---------- sidebar, theme, menu ---------- */
function expandSidebar(){ $('sidebar').classList.remove('collapsed'); $('sideToggle').textContent = '«'; localStorage.setItem(LS.collapsed, '0'); }
function closeMenu(){ $('ctxMenu').classList.remove('open'); }
function inject(text){
  const ta = $('input');
  ta.value = text;
  autosize();
  closeMenu();
  ta.focus();
}
function setTheme(t){
  document.documentElement.dataset.theme = t;
  $('themeBtn').textContent = t === 'light' ? '🌙' : '☀️';
  localStorage.setItem(LS.theme, t);
}
function restoreSidebar(){
  const w = parseInt(localStorage.getItem(LS.width) || '300', 10);
  if (w >= 180 && w <= 560) $('sidebar').style.width = w + 'px';
  if (localStorage.getItem(LS.collapsed) === '1'){ $('sidebar').classList.add('collapsed'); $('sideToggle').textContent = '»'; }
}

/* ---------- wiring ---------- */
$('sideToggle').onclick = function(){
  const sb = $('sidebar');
  sb.classList.toggle('collapsed');
  const collapsed = sb.classList.contains('collapsed');
  $('sideToggle').textContent = collapsed ? '»' : '«';
  localStorage.setItem(LS.collapsed, collapsed ? '1' : '0');
};
$('themeBtn').onclick = function(){ setTheme(document.documentElement.dataset.theme === 'light' ? 'dark' : 'light'); };

/* drag-resize */
(function(){
  const handle = $('sideResize');
  const sb = $('sidebar');
  let startX = 0, startW = 0, dragging = false;
  handle.addEventListener('mousedown', function(e){
    dragging = true; startX = e.clientX; startW = sb.getBoundingClientRect().width;
    sb.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    e.preventDefault();
  });
  window.addEventListener('mousemove', function(e){
    if (!dragging) return;
    const w = Math.max(180, Math.min(560, startW + (e.clientX - startX)));
    sb.style.width = w + 'px';
  });
  window.addEventListener('mouseup', function(){
    if (!dragging) return;
    dragging = false;
    sb.classList.remove('dragging');
    document.body.style.cursor = '';
    localStorage.setItem(LS.width, String(Math.round(sb.getBoundingClientRect().width)));
  });
})();

$('plusBtn').onclick = function(e){ e.stopPropagation(); $('ctxMenu').classList.toggle('open'); };
document.addEventListener('click', function(e){
  const menu = $('ctxMenu');
  if (!menu.contains(e.target) && e.target !== $('plusBtn')) closeMenu();
});
document.addEventListener('keydown', function(e){
  if (e.key === 'Escape'){
    closeMenu();
    if (S.busy) stopStream();
  }
});

document.querySelectorAll('[data-ctx]').forEach(function(btn){
  btn.onclick = function(){
    const k = btn.dataset.ctx;
    if (k === 'file'){
      const v = $('ctxFile').value.trim();
      if (!v){ $('ctxFile').focus(); return; }
      inject('/file ' + v); $('ctxFile').value = '';
    } else if (k === 'search'){
      const v = $('ctxSearch').value.trim();
      if (!v){ $('ctxSearch').focus(); return; }
      inject('/search ' + v); $('ctxSearch').value = '';
    } else if (k === 'url'){
      const v = $('ctxUrl').value.trim();
      if (!v){ $('ctxUrl').focus(); return; }
      inject('/url ' + v); $('ctxUrl').value = '';
    } else if (k === 'clear'){
      inject('/clear');
    }
  };
});
$('ctxModel').onchange = function(){ inject('/model ' + $('ctxModel').value); };

document.querySelectorAll('[data-quick]').forEach(function(btn){
  btn.onclick = function(){
    const k = btn.dataset.quick;
    if (k === 'file'){ $('filePick').click(); }
    else if (k === 'search'){ $('ctxMenu').classList.add('open'); $('ctxSearch').focus(); }
    else { $('ctxMenu').classList.toggle('open'); }
  };
});

$('sendBtn').onclick = send;
$('input').addEventListener('input', autosize);
$('input').addEventListener('keydown', function(e){
  if (e.key === 'Enter' && !e.shiftKey && !e.isComposing){ e.preventDefault(); send(); }
});
$('newChatBtn').onclick = clearSession;
$('exportBtn').onclick = exportChat;
$('healthBadge').onclick = function(){ pingHealth(true); };
$('modelSelect').onchange = function(){
  S.model = $('modelSelect').value;
  localStorage.setItem(LS.model, S.model);
  syncCtxModel();
  addSystem('🤖 [LOG] ' + TS() + ' | Model → ' + S.model + '.');
  toast('Model → ' + S.model);
};
$('wsApply').onclick = applyWorkspace;
$('previewRead').onclick = function(){ readPreview(false); };
$('previewPath').addEventListener('dblclick', function(){ readPreview(true); });
$('previewPath').addEventListener('keydown', function(e){
  if (e.key === 'Enter'){ e.preventDefault(); readPreview(false); }
});

/* attachments: picker + drag-drop */
$('clipBtn').onclick = function(){ $('filePick').click(); };
$('filePick').onchange = async function(){
  const files = [...this.files];
  for (const f of files){
    try {
      const text = await f.text();
      addPending(f.name, text);
      toast('Attached ' + f.name);
      addSystem('📎 [LOG] ' + TS() + ' | Attached file: ' + f.name + ' (' + text.length + ' chars).');
    } catch (e) { addSystem('🛑 [LOG] ' + TS() + ' | Cannot read ' + f.name + '.'); }
  }
  this.value = '';
  autosize();
};
const composer = $('composer');
['dragenter', 'dragover'].forEach(function(ev){
  composer.addEventListener(ev, function(e){ e.preventDefault(); composer.classList.add('dragover'); });
});
['dragleave', 'drop'].forEach(function(ev){
  composer.addEventListener(ev, function(e){ e.preventDefault(); composer.classList.remove('dragover'); });
});
composer.addEventListener('drop', async function(e){
  const files = [...(e.dataTransfer ? e.dataTransfer.files : [])];
  for (const f of files){
    try {
      const text = await f.text();
      addPending(f.name, text);
      toast('Attached ' + f.name);
      addSystem('📎 [LOG] ' + TS() + ' | Attached file: ' + f.name + ' (' + text.length + ' chars).');
    } catch (err) { addSystem('🛑 [LOG] ' + TS() + ' | Cannot read ' + f.name + '.'); }
  }
});

/* message actions */
$('chat').addEventListener('click', async function(e){
  const btn = e.target.closest('[data-a]');
  if (!btn) return;
  const holder = btn.closest('.msg');
  if (!holder) return;
  const idx = parseInt(holder.dataset.idx || '-1', 10);
  const m = S.messages[idx];
  if (!m) return;
  const a = btn.dataset.a;
  if (a === 'copy'){
    const ok = await copyText(m.content);
    showTip(btn, ok ? '✅ Copied' : '❌ Blocked');
    if (ok) toast('Copied');
  } else if (a === 'save'){
    download('ds3_reply_' + Date.now() + '.md', m.content);
    showTip(btn, '⬇ Saved');
    toast('Saved as .md');
  } else if (a === 'savews'){
    await saveToWorkspace(m);
  } else if (a === 'retry'){
    if (idx === S.messages.length - 1) await retryLast();
  } else if (a === 'edit'){
    if (m.role === 'user' && idx === S.messages.length - 1){
      $('input').value = m.content;
      S.editing = true;
      autosize();
      $('input').focus();
      toast('Editing — send to replace');
    }
  }
});

$('chat').addEventListener('scroll', updatePin);
$('jumpBtn').onclick = scrollBottom;

/* boot */
restoreSidebar();
setTheme(localStorage.getItem(LS.theme) || 'dark');
renderAll();
loadModels();
pingHealth(false);
loadTree();
refreshSessions();
autosize();
setInterval(function(){
  if (S.healthState !== 'ok') pingHealth(false);
}, 30000);
</script>
</body>
</html>
"""


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    global MAX_TURNS, WORKSPACE
    parser = argparse.ArgumentParser(description="DS3 — fast local UI for the Duck CLI")
    parser.add_argument("--port", type=int, default=APP_PORT)
    parser.add_argument("--host", default=APP_HOST)
    parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    parser.add_argument("--max-turns", type=int, default=MAX_TURNS,
                        help="turns of history kept in the prompt (0 = all)")
    args = parser.parse_args()
    MAX_TURNS = max(0, args.max_turns)
    try:
        wp = Path(args.workspace).expanduser().resolve()
        wp.mkdir(parents=True, exist_ok=True)
        WORKSPACE = wp
    except OSError:
        pass

    print("=" * 66)
    print("  DS3 Studio  ->  http://%s:%d" % (args.host, args.port))
    print("  Duck CLI    ->  %s" % CLI_BASE)
    print("  Workspace   ->  %s" % WORKSPACE)
    print("  Data        ->  %s" % DATA_DIR)
    print("=" * 66)
    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
