from dataclasses import dataclass
from typing import Optional

from .models import InstancePath


@dataclass
class Cache:
    """Simple in-memory cache for instance paths"""

    instance_paths: dict[str, InstancePath]

    def __init__(self):
        self.instance_paths = {}

    def get_instance_path(self, release_id: str) -> Optional[InstancePath]:
        return self.instance_paths.get(release_id)

    def save_instance_path(self, instance_path: InstancePath) -> None:
        self.instance_paths[instance_path.release_id] = instance_path
