from __future__ import annotations

import asyncio
from typing import Any, Optional

from .inventory import AstroInventory
from .models import InventorySnapshot


class SnapshotService:
    """Keeps a periodically refreshed snapshot in memory.

    Use this for marketplace APIs:
    - Call `start()` once at application startup
    - Serve `get_snapshot()` in your HTTP handlers
    - Call `stop()` during shutdown

    This avoids rescanning GitLab on every request.
    """

    def __init__(
        self,
        *,
        inventory: AstroInventory,
        logger: Any,
        refresh_interval_seconds: int = 300,
    ) -> None:
        self._inv = inventory
        self._logger = logger
        self._refresh_interval = int(refresh_interval_seconds)
        self._task: Optional[asyncio.Task] = None
        self._snapshot: Optional[InventorySnapshot] = None
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task
        self._task = None

    async def get_snapshot(self) -> Optional[InventorySnapshot]:
        async with self._lock:
            return self._snapshot

    async def refresh_now(self) -> InventorySnapshot:
        snapshot = await self._inv.get_inventory_snapshot()
        async with self._lock:
            self._snapshot = snapshot
        return snapshot

    async def _run_loop(self) -> None:
        # Initial refresh (best effort)
        try:
            await self.refresh_now()
        except Exception as e:
            self._logger.error("Initial snapshot refresh failed: %s", e)

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._refresh_interval)
                break
            except asyncio.TimeoutError:
                pass

            try:
                self._logger.debug("Refreshing inventory snapshot")
                await self.refresh_now()
                self._logger.debug("Inventory snapshot refreshed")
            except Exception as e:
                self._logger.error("Snapshot refresh failed: %s", e)
