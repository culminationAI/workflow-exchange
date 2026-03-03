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
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
    read_at TEXT
);
"""

VALID_AGENT_RE = re.compile(r"^[a-zA-Z0-9_]{1,50}$")
VALID_TYPES = {"task", "response", "notification", "config", "approval", "knowledge"}
VALID_PRIORITIES = {"low", "normal", "high"}
VALID_STATUSES = {"pending", "read", "processed", "archived", "approved", "rejected"}


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


class StatusUpdate(BaseModel):
    status: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}")
        return v


def row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


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

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO messages
                (id, from_agent, to_agent, type, priority, subject, body, in_reply_to, status, created_at, read_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL)
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
            ),
        )
        conn.commit()

        # Chain block
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        append_block(conn, dict(row))
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()

    # Background GitHub push
    background_tasks.add_task(sync_to_github)

    return row_to_dict(row)


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
    read_at = datetime.now(timezone.utc).isoformat() if payload.status == "read" else None

    with get_conn() as conn:
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Message not found")

        if read_at is not None:
            conn.execute(
                "UPDATE messages SET status = ?, read_at = ? WHERE id = ?",
                (payload.status, read_at, msg_id),
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
    blocks = []
    with open(CHAIN_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                blocks.append(json.loads(line))
    return list(reversed(blocks[-limit:]))


# ---------------------------------------------------------------------------
# SSE stream
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
