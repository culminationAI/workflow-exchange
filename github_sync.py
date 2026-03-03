"""GitHub streaming for blockchain chain file via Contents API."""

import base64
import logging
import os

import httpx

from pathlib import Path

logger = logging.getLogger("github_sync")

CHAIN_FILE = Path("/data/chain.jsonl") if Path("/data").exists() else Path("./chain.jsonl")
GITHUB_API = "https://api.github.com"


class GitHubSync:
    def __init__(self, token: str, repo: str, file_path: str = "chain.jsonl", branch: str = "main"):
        self.token = token
        self.repo = repo
        self.file_path = file_path
        self.branch = branch
        self.file_sha: str | None = None
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

    async def _get_file_sha(self, client: httpx.AsyncClient) -> str | None:
        url = f"{GITHUB_API}/repos/{self.repo}/contents/{self.file_path}"
        resp = await client.get(url, headers=self.headers, params={"ref": self.branch})
        if resp.status_code == 200:
            self.file_sha = resp.json()["sha"]
            return self.file_sha
        return None

    async def push(self, conn) -> bool:
        if not CHAIN_FILE.exists():
            return False

        content = CHAIN_FILE.read_bytes()
        b64_content = base64.b64encode(content).decode("ascii")
        block_count = content.count(b"\n")

        async with httpx.AsyncClient(timeout=30.0) as client:
            await self._get_file_sha(client)

            url = f"{GITHUB_API}/repos/{self.repo}/contents/{self.file_path}"
            payload = {
                "message": f"chain: sync {block_count} blocks",
                "content": b64_content,
                "branch": self.branch,
            }
            if self.file_sha:
                payload["sha"] = self.file_sha

            resp = await client.put(url, headers=self.headers, json=payload)

            if resp.status_code in (200, 201):
                new_sha = resp.json()["content"]["sha"]
                self.file_sha = new_sha
                conn.execute(
                    "INSERT OR REPLACE INTO chain_meta (key, value) VALUES ('github_file_sha', ?)",
                    (new_sha,),
                )
                conn.commit()
                logger.info(f"GitHub sync OK: {block_count} blocks, sha={new_sha[:12]}")
                return True
            else:
                logger.error(f"GitHub sync failed: {resp.status_code} {resp.text[:200]}")
                return False
