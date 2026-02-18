from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from .models import InstancePath

T = TypeVar("T")


@dataclass
class _TtlEntry(Generic[T]):
    value: T
    expires_at: float


class TtlCache(Generic[T]):
    """Simple in-memory TTL cache.

    Notes:
    - Caches are per-process. If you run multiple workers, each has its own cache.
    - We cache *None* too when needed (e.g., missing files), to reduce repeated 404 calls.
    """

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl = float(ttl_seconds)
        self._data: dict[str, _TtlEntry[T]] = {}

    def get(self, key: str) -> Optional[T]:
        entry = self._data.get(key)
        if entry is None:
            return None
        now = time.time()
        if entry.expires_at < now:
            self._data.pop(key, None)
            return None
        return entry.value

    def set(self, key: str, value: T) -> None:
        self._data[key] = _TtlEntry(value=value, expires_at=time.time() + self._ttl)

    def clear(self) -> None:
        self._data.clear()


class InMemoryCache:
    """Minimal cache: release_id -> InstancePath.

    This is useful for repeated lookups, but marketplace use cases should use bulk scanning.
    """

    def __init__(self) -> None:
        self._by_release_id: dict[str, InstancePath] = {}

    def get_instance_path(self, release_id: str) -> Optional[InstancePath]:
        return self._by_release_id.get(release_id)

    def save_instance_path(self, inst_path: InstancePath) -> None:
        self._by_release_id[inst_path.release_id] = inst_path
