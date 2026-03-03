"""
Inter-agent exchange web service.
FastAPI + SQLite backend for OkiAra <-> FalkVelt message passing.
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from chain import append_block, verify_chain, GENESIS_HASH, CHAIN_FILE
from github_sync import GitHubSync

# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

DB_PATH = "/data/exchange.db" if Path("/data").exists() else "./exchange.db"

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_CHAIN_REPO = os.environ.get("GITHUB_CHAIN_REPO", "culminationAI/agent-exchange-chain")
github_sync = GitHubSync(GITHUB_TOKEN, GITHUB_CHAIN_REPO) if GITHUB_TOKEN else None

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'notification',
    priority TEXT NOT NULL DEFAULT 'normal',
    subject TEXT NOT NULL,
    body TEXT DEFAULT '',
    in_reply_to TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    read_at TEXT,
    accepted_at TEXT
);
"""

VALID_AGENT_RE = re.compile(r"^[a-zA-Z0-9_]{1,50}$")
VALID_TYPES = {"task", "response", "notification", "config", "approval", "knowledge"}
VALID_PRIORITIES = {"low", "normal", "high"}
VALID_STATUSES = {"pending", "read", "accepted", "processed", "archived", "approved", "rejected"}

# Activities: in-memory, ephemeral, max 200
activities_store: deque = deque(maxlen=200)
activities_subscribers: list = []  # SSE subscriber queues

# Presence: in-memory, 90s timeout
presence_store: Dict[str, Dict[str, Any]] = {}

# Approve mode: in-memory, per-agent
approve_mode_store: dict = {}

# Message SSE subscribers (for UI broadcast)
message_subscribers: list = []


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(CREATE_TABLE_SQL)

        # Chain columns migration (idempotent)
        for col, coltype in [("block_number", "INTEGER"), ("block_hash", "TEXT"), ("prev_hash", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE messages ADD COLUMN {col} {coltype}")
            except sqlite3.OperationalError:
                pass  # column already exists

        # Payload column migration (idempotent)
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN payload TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

        # accepted_at column migration (idempotent)
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN accepted_at TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

        # Chain metadata table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chain_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        conn.commit()

        # Retroactively chain existing unchained messages
        unchained = conn.execute(
            "SELECT * FROM messages WHERE block_number IS NULL ORDER BY created_at ASC"
        ).fetchall()
        for row in unchained:
            append_block(conn, dict(row))


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Agent Exchange", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class MessageCreate(BaseModel):
    from_agent: str
    to_agent: str
    type: str = "notification"
    priority: str = "normal"
    subject: str
    body: str = ""
    in_reply_to: Optional[str] = None
    payload: Optional[dict] = None

    @field_validator("from_agent", "to_agent")
    @classmethod
    def validate_agent_name(cls, v: str) -> str:
        if not VALID_AGENT_RE.match(v):
            raise ValueError("Agent name must be alphanumeric + underscore, 1-50 chars")
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_TYPES:
            raise ValueError(f"type must be one of {VALID_TYPES}")
        return v

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: str) -> str:
        if v not in VALID_PRIORITIES:
            raise ValueError(f"priority must be one of {VALID_PRIORITIES}")
        return v

    @field_validator("subject")
    @classmethod
    def validate_subject(cls, v: str) -> str:
        if len(v) > 200:
            raise ValueError("subject max 200 chars")
        return v

    @field_validator("body")
    @classmethod
    def validate_body(cls, v: str) -> str:
        if len(v) > 10000:
            raise ValueError("body max 10000 chars")
        return v

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, v: Optional[dict]) -> Optional[dict]:
        if v is not None and len(json.dumps(v)) > 15000:
            raise ValueError("payload serialized max 15000 chars")
        return v


class StatusUpdate(BaseModel):
    status: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}")
        return v


class ActivityCreate(BaseModel):
    agent: str
    type: str  # thinking, executing, delegating, waiting, completed, error
    detail: str = ""
    parent_id: Optional[str] = None

    @field_validator("agent")
    @classmethod
    def validate_agent(cls, v: str) -> str:
        if not VALID_AGENT_RE.match(v):
            raise ValueError("Invalid agent name")
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in {"thinking", "executing", "delegating", "waiting", "completed", "error"}:
            raise ValueError("Invalid activity type")
        return v


class PresenceUpdate(BaseModel):
    state: str = "online"
    version: Optional[str] = None

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        if v not in {"online", "busy", "offline"}:
            raise ValueError("state must be online, busy, or offline")
        return v


class ApproveModeUpdate(BaseModel):
    agent: str
    enabled: bool
    duration_minutes: int = 60

    @field_validator("duration_minutes")
    @classmethod
    def validate_duration(cls, v: int) -> int:
        if v not in {0, 30, 60}:
            raise ValueError("duration must be 0 (session), 30, or 60")
        return v


def row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    # Parse payload JSON if stored as string
    if d.get("payload") and isinstance(d["payload"], str):
        try:
            d["payload"] = json.loads(d["payload"])
        except json.JSONDecodeError:
            pass
    return d


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    template_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(content=template_path.read_text(encoding="utf-8"))


@app.post("/messages", status_code=201)
async def create_message(payload: MessageCreate, background_tasks: BackgroundTasks) -> dict:
    msg_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(payload.payload) if payload.payload else None

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO messages
                (id, from_agent, to_agent, type, priority, subject, body, in_reply_to, status, created_at, read_at, payload)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?)
            """,
            (
                msg_id,
                payload.from_agent,
                payload.to_agent,
                payload.type,
                payload.priority,
                payload.subject,
                payload.body,
                payload.in_reply_to,
                created_at,
                payload_json,
            ),
        )
        conn.commit()

        # Chain block
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        append_block(conn, dict(row))

        # Auto-approve if approve mode is active for the sender
        if payload.type == "approval":
            am = approve_mode_store.get(payload.from_agent)
            if am and am["enabled"] and (am["expires_at"] == 0 or am["expires_at"] > time.time()):
                conn.execute("UPDATE messages SET status = 'approved' WHERE id = ?", (msg_id,))
                conn.commit()

        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        msg_dict = row_to_dict(row)

        # Broadcast to message SSE subscribers
        for q in message_subscribers:
            try:
                q.put_nowait(msg_dict)
            except Exception:
                pass

    # Background GitHub push
    background_tasks.add_task(sync_to_github)

    return msg_dict


@app.get("/messages")
async def list_messages(
    to: Optional[str] = Query(default=None),
    from_agent: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> list:
    filters = []
    params: list = []

    if to:
        filters.append("to_agent = ?")
        params.append(to)
    if from_agent:
        filters.append("from_agent = ?")
        params.append(from_agent)
    if status:
        if status not in VALID_STATUSES:
            raise HTTPException(status_code=422, detail=f"status must be one of {VALID_STATUSES}")
        filters.append("status = ?")
        params.append(status)
    if type:
        if type not in VALID_TYPES:
            raise HTTPException(status_code=422, detail=f"type must be one of {VALID_TYPES}")
        filters.append("type = ?")
        params.append(type)

    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM messages {where_clause} ORDER BY created_at DESC LIMIT ?",
            params,
        ).fetchall()

    return [row_to_dict(r) for r in rows]


@app.get("/messages/{msg_id}")
async def get_message(msg_id: str) -> dict:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Message not found")

    return row_to_dict(row)


@app.patch("/messages/{msg_id}")
async def update_message_status(msg_id: str, payload: StatusUpdate) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    read_at = now if payload.status == "read" else None
    accepted_at = now if payload.status == "accepted" else None

    with get_conn() as conn:
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Message not found")

        if read_at is not None:
            conn.execute(
                "UPDATE messages SET status = ?, read_at = ? WHERE id = ?",
                (payload.status, read_at, msg_id),
            )
        elif accepted_at is not None:
            conn.execute(
                "UPDATE messages SET status = ?, accepted_at = ? WHERE id = ?",
                (payload.status, accepted_at, msg_id),
            )
        else:
            conn.execute(
                "UPDATE messages SET status = ? WHERE id = ?",
                (payload.status, msg_id),
            )
        conn.commit()
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()

    return row_to_dict(row)


@app.delete("/messages/{msg_id}", status_code=204)
async def archive_message(msg_id: str) -> Response:
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM messages WHERE id = ?", (msg_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Message not found")

        conn.execute("UPDATE messages SET status = 'archived' WHERE id = ?", (msg_id,))
        conn.commit()

    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Chain endpoints
# ---------------------------------------------------------------------------

async def sync_to_github():
    """Background task: push chain to GitHub."""
    if github_sync is None:
        return
    try:
        with get_conn() as conn:
            await github_sync.push(conn)
    except Exception as e:
        logging.getLogger("exchange").error(f"GitHub sync error: {e}")


@app.get("/chain/verify")
async def verify_chain_endpoint() -> dict:
    return verify_chain()


@app.get("/chain/status")
async def chain_status() -> dict:
    with get_conn() as conn:
        meta = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM chain_meta").fetchall()}
    return {
        "last_block_number": int(meta.get("last_block_number", 0)),
        "last_block_hash": meta.get("last_block_hash", GENESIS_HASH)[:16] + "...",
        "github_synced": "github_file_sha" in meta,
        "github_file_sha": (meta.get("github_file_sha", "")[:12] + "...") if "github_file_sha" in meta else None,
    }


@app.get("/chain/blocks")
async def list_blocks(limit: int = Query(default=50, ge=1, le=500)) -> list:
    if not CHAIN_FILE.exists():
        return []
    # Use a bounded deque to avoid Pyright slice-with-variable false positives
    window: deque = deque(maxlen=limit)
    with open(CHAIN_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                window.append(json.loads(line))
    return list(reversed(list(window)))


# ---------------------------------------------------------------------------
# Activities endpoints
# ---------------------------------------------------------------------------

@app.post("/activities", status_code=201)
async def create_activity(payload: ActivityCreate) -> dict:
    activity = {
        "id": str(uuid.uuid4()),
        "agent": payload.agent,
        "type": payload.type,
        "detail": payload.detail,
        "parent_id": payload.parent_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    activities_store.append(activity)
    for q in activities_subscribers:
        try:
            q.put_nowait(activity)
        except Exception:
            pass
    return activity


@app.get("/activities")
async def list_activities(
    agent: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> list:
    cutoff = time.time() - 3600
    # Use a bounded deque so we never need slice notation (Pyright rejects list slicing with variables)
    window: deque = deque(maxlen=limit)
    for a in activities_store:
        ts = datetime.fromisoformat(a["created_at"]).timestamp()
        if ts < cutoff:
            continue
        if agent and a["agent"] != agent:
            continue
        window.append(a)
    return list(reversed(list(window)))


@app.get("/stream/activities")
async def stream_activities():
    """SSE stream for real-time activity delivery."""
    queue: asyncio.Queue = asyncio.Queue()
    activities_subscribers.append(queue)

    async def gen():
        try:
            while True:
                try:
                    act = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {json.dumps(act)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in activities_subscribers:
                activities_subscribers.remove(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Presence endpoints
# ---------------------------------------------------------------------------

@app.post("/presence/{agent}")
async def update_presence(agent: str, body: PresenceUpdate = PresenceUpdate()) -> dict:
    if not VALID_AGENT_RE.match(agent):
        raise HTTPException(422, "Invalid agent name")
    version = body.version or (presence_store.get(agent, {}).get("version"))
    presence_store[agent] = {
        "agent": agent, "state": body.state, "updated_at": time.time(),
        **({"version": version} if version else {}),
    }
    return presence_store[agent]


@app.get("/presence/{agent}")
async def get_presence(agent: str) -> Dict[str, Any]:
    entry = presence_store.get(agent)
    if entry is None or (time.time() - entry["updated_at"] > 90):
        return {"agent": agent, "state": "offline", "updated_at": None}
    return entry


@app.get("/presence")
async def get_all_presence() -> dict:
    now = time.time()
    return {
        a: {**e, "state": "offline" if now - e["updated_at"] > 90 else e["state"]}
        for a, e in presence_store.items()
    }


# ---------------------------------------------------------------------------
# Approve mode endpoints
# ---------------------------------------------------------------------------

@app.get("/settings/approve-mode")
async def get_approve_mode() -> dict:
    now = time.time()
    result = {}
    for agent, s in approve_mode_store.items():
        active = s["enabled"] and (s["expires_at"] == 0 or s["expires_at"] > now)
        result[agent] = {
            "enabled": active,
            "expires_at": s["expires_at"],
            "remaining_seconds": max(0, int(s["expires_at"] - now)) if s["expires_at"] > 0 else None,
        }
    return result


@app.patch("/settings/approve-mode")
async def update_approve_mode(body: ApproveModeUpdate) -> dict:
    if body.enabled:
        expires = time.time() + body.duration_minutes * 60 if body.duration_minutes > 0 else 0
        approve_mode_store[body.agent] = {"enabled": True, "expires_at": expires}
    else:
        approve_mode_store[body.agent] = {"enabled": False, "expires_at": 0}
    return approve_mode_store[body.agent]


# ---------------------------------------------------------------------------
# SSE stream — messages broadcast
# ---------------------------------------------------------------------------

@app.get("/stream/messages")
async def stream_all_messages():
    """SSE stream broadcasting every new message to all subscribers."""
    queue: asyncio.Queue = asyncio.Queue()
    message_subscribers.append(queue)

    async def gen():
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in message_subscribers:
                message_subscribers.remove(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# SSE stream — per-agent (existing)
# ---------------------------------------------------------------------------

@app.get("/stream")
async def stream(agent: str = Query(...)):
    """Server-Sent Events stream for real-time message delivery."""

    async def event_generator():
        seen: set[str] = set()
        while True:
            with get_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM messages WHERE to_agent = ? AND status = 'pending' ORDER BY created_at ASC",
                    (agent,),
                ).fetchall()
            for row in rows:
                msg = row_to_dict(row)
                if msg["id"] not in seen:
                    seen.add(msg["id"])
                    yield f"data: {json.dumps(msg)}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
