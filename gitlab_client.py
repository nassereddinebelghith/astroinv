from __future__ import annotations

import asyncio
import base64
import urllib.parse
from dataclasses import dataclass
from typing import Any, Optional

from aiohttp import ClientSession
from tenacity import retry, stop_after_attempt, wait_fixed

from .cache import TtlCache
from .constants import (
    FILE_CACHE_TTL_SECONDS,
    GITLAB_MAX_TRIES,
    GITLAB_TRY_WAIT_SECONDS,
    TREE_CACHE_TTL_SECONDS,
)
from .exceptions import GitLabError


@dataclass(frozen=True)
class GitLabFile:
    path: str
    content: str


class GitLabClient:
    """GitLab API client with performance features.

    - Shared aiohttp session (connection pooling)
    - Bounded concurrency (Semaphore)
    - TTL caches for tree and file reads
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        logger: Any,
        session: ClientSession,
        concurrency: int,
        tree_cache_ttl_seconds: int = TREE_CACHE_TTL_SECONDS,
        file_cache_ttl_seconds: int = FILE_CACHE_TTL_SECONDS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.logger = logger
        self._session = session
        self._sem = asyncio.Semaphore(max(1, int(concurrency)))

        self._tree_cache: TtlCache[list[dict]] = TtlCache(ttl_seconds=tree_cache_ttl_seconds)
        self._file_cache: TtlCache[GitLabFile | None] = TtlCache(ttl_seconds=file_cache_ttl_seconds)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _tree_key(self, project_id: int, path: str, ref: str) -> str:
        return f"tree:{project_id}:{ref}:{path}"

    def _file_key(self, project_id: int, path: str, ref: str) -> str:
        return f"file:{project_id}:{ref}:{path}"

    async def _bounded(self, coro):
        async with self._sem:
            return await coro

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def repository_tree(self, *, project_id: int, path: str, ref: str) -> list[dict]:
        cache_key = self._tree_key(project_id, path, ref)
        cached = self._tree_cache.get(cache_key)
        if cached is not None:
            return cached

        async def _do():
            self.logger.debug("GitLab tree: project=%s ref=%s path=%s", project_id, ref, path)
            url = f"{self.base_url}/projects/{project_id}/repository/tree"
            tree: list[dict] = []
            page = 1

            while True:
                async with self._session.get(
                    url,
                    headers=self._headers(),
                    params={"page": page, "path": path, "per_page": 100, "ref": ref},
                ) as resp:
                    if resp.status != 200:
                        raise GitLabError(resp.status, await resp.text())
                    tree.extend(await resp.json())
                    next_page = resp.headers.get("X-Next-Page", "")
                    if not next_page:
                        break
                    page = int(next_page)

            self._tree_cache.set(cache_key, tree)
            return tree

        return await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def get_file(self, *, project_id: int, path: str, ref: str) -> Optional[GitLabFile]:
        cache_key = self._file_key(project_id, path, ref)
        cached = self._file_cache.get(cache_key)
        if cached is not None or cache_key in self._file_cache._data:
            return cached  # type: ignore[return-value]

        async def _do():
            self.logger.debug("GitLab file: project=%s ref=%s path=%s", project_id, ref, path)
            url_path = urllib.parse.quote(path, safe="")
            url = f"{self.base_url}/projects/{project_id}/repository/files/{url_path}"
            async with self._session.get(url, headers=self._headers(), params={"ref": ref}) as resp:
                if resp.status == 404:
                    self._file_cache.set(cache_key, None)
                    return None
                if resp.status != 200:
                    raise GitLabError(resp.status, await resp.text())
                data = await resp.json()

            content_b64 = data.get("content", "")
            try:
                content = base64.b64decode(content_b64).decode("utf-8")
            except Exception:
                content = urllib.parse.unquote(content_b64)

            f = GitLabFile(path=path, content=content)
            self._file_cache.set(cache_key, f)
            return f

        return await self._bounded(_do())

    def _invalidate(self, project_id: int, ref: str, path: str) -> None:
        self._file_cache.set(self._file_key(project_id, path, ref), None)
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        self._tree_cache.set(self._tree_key(project_id, parent, ref), [])

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def create_branch(self, *, project_id: int, branch: str, ref: str) -> None:
        async def _do():
            url = f"{self.base_url}/projects/{project_id}/repository/branches"
            async with self._session.post(url, headers=self._headers(), json={"branch": branch, "ref": ref}) as resp:
                if resp.status != 201:
                    raise GitLabError(resp.status, await resp.text())
        await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def delete_branch(self, *, project_id: int, branch: str) -> None:
        async def _do():
            url_branch = urllib.parse.quote(branch, safe="")
            url = f"{self.base_url}/projects/{project_id}/repository/branches/{url_branch}"
            async with self._session.delete(url, headers=self._headers()) as resp:
                if resp.status != 204:
                    raise GitLabError(resp.status, await resp.text())
        await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def create_file(
        self,
        *,
        project_id: int,
        path: str,
        content: str,
        branch: str,
        commit_message: str,
        author_name: str,
        author_email: str,
    ) -> None:
        async def _do():
            url_path = urllib.parse.quote(path, safe="")
            url = f"{self.base_url}/projects/{project_id}/repository/files/{url_path}"
            async with self._session.post(
                url,
                headers=self._headers(),
                json={
                    "branch": branch,
                    "content": content,
                    "author_email": author_email,
                    "author_name": author_name,
                    "commit_message": commit_message,
                },
            ) as resp:
                if resp.status != 201:
                    raise GitLabError(resp.status, await resp.text())
            self._invalidate(project_id, branch, path)
        await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def update_file(
        self,
        *,
        project_id: int,
        path: str,
        content: str,
        branch: str,
        commit_message: str,
        author_name: str,
        author_email: str,
    ) -> None:
        async def _do():
            url_path = urllib.parse.quote(path, safe="")
            url = f"{self.base_url}/projects/{project_id}/repository/files/{url_path}"
            async with self._session.put(
                url,
                headers=self._headers(),
                json={
                    "branch": branch,
                    "content": content,
                    "author_email": author_email,
                    "author_name": author_name,
                    "commit_message": commit_message,
                },
            ) as resp:
                if resp.status != 200:
                    raise GitLabError(resp.status, await resp.text())
            self._invalidate(project_id, branch, path)
        await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def create_merge_request(
        self,
        *,
        project_id: int,
        source_branch: str,
        target_branch: str,
        title: str,
    ) -> int:
        async def _do():
            url = f"{self.base_url}/projects/{project_id}/merge_requests"
            async with self._session.post(
                url,
                headers=self._headers(),
                json={"source_branch": source_branch, "target_branch": target_branch, "title": title},
            ) as resp:
                if resp.status != 201:
                    raise GitLabError(resp.status, await resp.text())
                data = await resp.json()
            return int(data.get("iid") or data["id"])
        return await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def rebase_merge_request(self, *, project_id: int, mr_id: int) -> None:
        async def _do():
            url = f"{self.base_url}/projects/{project_id}/merge_requests/{mr_id}/rebase"
            async with self._session.put(url, headers=self._headers()) as resp:
                if resp.status not in (200, 202):
                    raise GitLabError(resp.status, await resp.text())
        await self._bounded(_do())

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT_SECONDS), stop=stop_after_attempt(GITLAB_MAX_TRIES))
    async def merge_merge_request(self, *, project_id: int, mr_id: int) -> None:
        async def _do():
            url = f"{self.base_url}/projects/{project_id}/merge_requests/{mr_id}/merge"
            async with self._session.put(url, headers=self._headers()) as resp:
                if resp.status != 200:
                    raise GitLabError(resp.status, await resp.text())
        await self._bounded(_do())
