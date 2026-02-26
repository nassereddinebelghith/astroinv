from ._imports import *
from .constants import *
from .errors import *

class Cache:
    def get_cluster(self, name: str) -> Cluster | None:
        pass

    def get_instance_path(self, release_id: str) -> InstancePath | None:
        return None

    def save_cluster(self, cluster: Cluster):
        pass

    def save_instance_path(self, path: InstancePath):
        pass

class InMemoryCache(Cache, BaseModel):
    clusters: dict[str, Cluster] = {}
    paths: dict[str, InstancePath] = {}
    def get_cluster(self, name: str) -> Cluster | None:
        return self.clusters.get(name)

    def get_instance_path(self, release_id: str) -> InstancePath | None:
        return self.paths.get(release_id)

    def save_cluster(self, cluster: Cluster):
        self.clusters[cluster.name] = cluster
    def save_instance_path(self, path: InstancePath):
        self.paths[path.release_id] = path

class FileSystemCache(Cache):
    cache: InMemoryCache
    path: Path
    def init(self, path: Path):
        self.path = path
        if self.path.is_file():
            with open(self.path, "r") as cache_file:
                self.cache = InMemoryCache.model_validate_json(cache_file.read())
        else:
            self.cache = InMemoryCache()
    def get_cluster(self, name: str) -> Cluster | None:
        return self.cache.get_cluster(name)
    def get_instance_path(self, release_id: str) -> InstancePath | None:
        return self.cache.get_instance_path(release_id)
    def save(self):
        json = self.cache.model_dump_json()
        with open(self.path, "w") as cache_file:
            cache_file.write(json)
    def save_cluster(self, cluster: Cluster):
        self.cache.save_cluster(cluster)
    def save_instance_path(self, path: InstancePath):
        self.cache.save_instance_path(path)

