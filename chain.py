"""Blockchain hash chain for inter-agent exchange messages."""

import hashlib
import json
from pathlib import Path

CHAIN_FILE = Path("/data/chain.jsonl") if Path("/data").exists() else Path("./chain.jsonl")
GENESIS_HASH = "0" * 64


def compute_body_hash(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def compute_block_hash(
    prev_hash: str, block_number: int, timestamp: str,
    message_id: str, from_agent: str, to_agent: str,
    msg_type: str, subject: str, body_hash: str,
) -> str:
    payload = f"{prev_hash}|{block_number}|{timestamp}|{message_id}|{from_agent}|{to_agent}|{msg_type}|{subject}|{body_hash}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_chain_state(conn) -> tuple:
    """Return (last_block_number, last_block_hash) from chain_meta."""
    row = conn.execute("SELECT value FROM chain_meta WHERE key = 'last_block_number'").fetchone()
    if row is None:
        return (0, GENESIS_HASH)
    block_num = int(row[0])
    hash_row = conn.execute("SELECT value FROM chain_meta WHERE key = 'last_block_hash'").fetchone()
    return (block_num, hash_row[0])


def append_block(conn, message: dict) -> dict:
    """Create a new block from a message, append to chain file, update DB. Returns block dict."""
    last_num, last_hash = get_chain_state(conn)
    new_num = last_num + 1
    timestamp = message["created_at"]
    body_hash = compute_body_hash(message.get("body", "") or "")

    block_hash = compute_block_hash(
        last_hash, new_num, timestamp,
        message["id"], message["from_agent"], message["to_agent"],
        message["type"], message["subject"], body_hash,
    )

    block = {
        "block_number": new_num,
        "hash": block_hash,
        "prev_hash": last_hash,
        "timestamp": timestamp,
        "message_id": message["id"],
        "from_agent": message["from_agent"],
        "to_agent": message["to_agent"],
        "type": message["type"],
        "priority": message["priority"],
        "subject": message["subject"],
        "body_hash": body_hash,
        "body_length": len(message.get("body", "") or ""),
    }

    # Append to JSONL
    with open(CHAIN_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(block, ensure_ascii=False) + "\n")

    # Update SQLite
    conn.execute(
        "UPDATE messages SET block_number=?, block_hash=?, prev_hash=? WHERE id=?",
        (new_num, block_hash, last_hash, message["id"]),
    )
    conn.execute(
        "INSERT OR REPLACE INTO chain_meta (key, value) VALUES ('last_block_number', ?)",
        (str(new_num),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO chain_meta (key, value) VALUES ('last_block_hash', ?)",
        (block_hash,),
    )
    conn.commit()

    return block


def verify_chain() -> dict:
    """Read chain.jsonl and verify every block's hash + prev_hash linkage."""
    if not CHAIN_FILE.exists():
        return {"valid": True, "blocks": 0, "errors": []}

    errors = []
    prev_hash = GENESIS_HASH
    count = 0

    with open(CHAIN_FILE, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                block = json.loads(line)
            except json.JSONDecodeError:
                errors.append(f"Line {line_num}: invalid JSON")
                continue

            count += 1

            if block["prev_hash"] != prev_hash:
                errors.append(
                    f"Block {block['block_number']}: prev_hash mismatch "
                    f"(expected {prev_hash[:16]}..., got {block['prev_hash'][:16]}...)"
                )

            expected = compute_block_hash(
                block["prev_hash"], block["block_number"], block["timestamp"],
                block["message_id"], block["from_agent"], block["to_agent"],
                block["type"], block["subject"], block["body_hash"],
            )
            if block["hash"] != expected:
                errors.append(f"Block {block['block_number']}: hash mismatch")

            prev_hash = block["hash"]

    return {"valid": len(errors) == 0, "blocks": count, "errors": errors}
