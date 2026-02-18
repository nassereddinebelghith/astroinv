"""astroinv

Optimized, marketplace-oriented inventory scanning library (best-effort reconstruction).

Key properties:
- Shared aiohttp session
- Bounded concurrency for GitLab calls
- TTL caching for repository trees and file reads
- Bulk scanning API for marketplace use cases
- Safe YAML parsing
"""

from .models import (
    Service,
    Cluster,
    Customer,
    Secret,
    SecretStore,
    Source,
    InstancePath,
    Instance,
    Ephemeral,
    AirflowHealthError,
    AirflowHealthResult,
    EphemeralConfig,
    InventorySnapshot,
)
from .cache import InMemoryCache, TtlCache
from .gitlab_client import GitLabClient, GitLabFile
from .inventory import AstroInventory
from .snapshot import SnapshotService

__all__ = [
    "Service",
    "Cluster",
    "Customer",
    "Secret",
    "SecretStore",
    "Source",
    "InstancePath",
    "Instance",
    "Ephemeral",
    "AirflowHealthError",
    "AirflowHealthResult",
    "EphemeralConfig",
    "InventorySnapshot",
    "InMemoryCache",
    "TtlCache",
    "GitLabClient",
    "GitLabFile",
    "AstroInventory",
    "SnapshotService",
]
