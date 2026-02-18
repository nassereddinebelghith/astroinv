from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any, Optional

import aiohttp
import yaml

from .cache import InMemoryCache
from .constants import CLUSTER_REGEX, DEFAULT_GITLAB_CONCURRENCY, DEFAULT_HEALTH_CONCURRENCY
from .exceptions import InstanceNotFoundError, InvalidClusterNameError, InvalidInventoryPathError
from .gitlab_client import GitLabClient
from .helpers import ephemeral_name, instance_name, is_database_secret_creation_enabled, url_from_name
from .models import (
    AirflowHealthResult,
    Cluster,
    Customer,
    Ephemeral,
    EphemeralConfig,
    Instance,
    InstancePath,
    InventorySnapshot,
    Service,
    Source,
)
from .health import get_health


class AstroInventory:
    """Marketplace-oriented inventory reader."""

    def __init__(
        self,
        *,
        gitlab_url: str,
        gitlab_token: str,
        inv_project_id: int,
        inv_ref: str,
        legacy_inv_project_id: int,
        cluster_project_id: int,
        cluster_ref: str,
        logger: Any,
        cache: Optional[InMemoryCache] = None,
        gitlab_concurrency: int = DEFAULT_GITLAB_CONCURRENCY,
        health_concurrency: int = DEFAULT_HEALTH_CONCURRENCY,
    ) -> None:
        self.inv_project_id = inv_project_id
        self.inv_ref = inv_ref
        self.legacy_inv_project_id = legacy_inv_project_id
        self.cluster_project_id = cluster_project_id
        self.cluster_ref = cluster_ref
        self.logger = logger
        self.cache = cache or InMemoryCache()

        self._session = aiohttp.ClientSession()

        self.gitlab = GitLabClient(
            base_url=gitlab_url,
            token=gitlab_token,
            logger=logger,
            session=self._session,
            concurrency=gitlab_concurrency,
        )

        self._health_sem = asyncio.Semaphore(max(1, int(health_concurrency)))

    async def aclose(self) -> None:
        await self._session.close()

    # -----------------------------
    # Marketplace APIs
    # -----------------------------

    async def get_inventory_snapshot(self) -> InventorySnapshot:
        instances = await self.get_all_instances()
        return InventorySnapshot(instances=instances, generated_at_utc=dt.datetime.utcnow().isoformat() + "Z")

    async def get_all_instances(self) -> list[Instance]:
        instance_paths = await self._collect_all_instance_paths(zones=("hprd", "prod"))
        instances = await asyncio.gather(*(self._parse_instance(ip) for ip in instance_paths))
        return list(instances)

    async def get_all_instances_with_health(self) -> list[AirflowHealthResult]:
        instances = await self.get_all_instances()
        return await asyncio.gather(*(self._health_bounded(i) for i in instances))

    async def _health_bounded(self, inst: Instance) -> AirflowHealthResult:
        async with self._health_sem:
            return await get_health(self.logger, self._session, inst)

    # -----------------------------
    # Compatibility API
    # -----------------------------

    async def get_by_release_id(self, release_id: str) -> Optional[Instance]:
        ip = self.cache.get_instance_path(release_id)
        if ip is not None:
            return await self._parse_instance(ip)

        instance_paths = await self._collect_all_instance_paths(zones=("hprd", "prod"))
        for ip in instance_paths:
            if ip.release_id == release_id:
                return await self._parse_instance(ip)
        return None

    # -----------------------------
    # Ephemeral workflow (best-effort)
    # -----------------------------

    async def save_ephemeral(
        self,
        cfg: EphemeralConfig,
        *,
        author_name: str,
        author_email: str,
        dry_run: bool = False,
        no_merge: bool = False,
    ) -> Ephemeral:
        inst = await self.get_by_release_id(cfg.source)
        if inst is None:
            raise InstanceNotFoundError(cfg.source)

        env = inst.env
        if env == "dev" and inst.customer.name == "datahub":
            env = "dhdev"

        eph_name = ephemeral_name(inst.name, cfg.number)
        values_path = f"{inst.zone}/{env}/{inst.customer.name}-{inst.customer.apcode}/{inst.apcode}-{inst.release_id}/eph{cfg.number}.yaml"

        eph = Ephemeral(
            instance=inst,
            name=eph_name,
            number=cfg.number,
            source=Source(path=values_path, project_id=self.inv_project_id, ref=self.inv_ref, values={}),
            bucket_sync=cfg.bucket_sync,
            git_sync=cfg.git_sync,
            secret_stores=None,
        )

        values_content = yaml.safe_dump({"bucketSync": cfg.bucket_sync, "gitSync": cfg.git_sync}, sort_keys=True)

        if dry_run:
            return eph

        now = dt.datetime.now()
        branch = f"{inst.release_id}-{cfg.number}-{now.strftime('%Y%m%d%H%M%S')}"
        await self.gitlab.create_branch(project_id=self.inv_project_id, branch=branch, ref=self.inv_ref)

        try:
            existing = await self.gitlab.get_file(project_id=self.inv_project_id, path=values_path, ref=branch)

            changed = False
            if existing is None:
                mr_title = f"{eph.name}: create"
                await self.gitlab.create_file(
                    project_id=self.inv_project_id,
                    path=values_path,
                    content=values_content,
                    branch=branch,
                    commit_message=f"{eph.name}: add values",
                    author_name=author_name,
                    author_email=author_email,
                )
                changed = True
            else:
                mr_title = f"{eph.name}: update"
                if existing.content != values_content:
                    await self.gitlab.update_file(
                        project_id=self.inv_project_id,
                        path=values_path,
                        content=values_content,
                        branch=branch,
                        commit_message=f"{eph.name}: update values",
                        author_name=author_name,
                        author_email=author_email,
                    )
                    changed = True

            if changed:
                mr_id = await self.gitlab.create_merge_request(
                    project_id=self.inv_project_id,
                    source_branch=branch,
                    target_branch=self.inv_ref,
                    title=mr_title,
                )
                if not no_merge:
                    await self.gitlab.rebase_merge_request(project_id=self.inv_project_id, mr_id=mr_id)
                    await self.gitlab.merge_merge_request(project_id=self.inv_project_id, mr_id=mr_id)
            else:
                await self.gitlab.delete_branch(project_id=self.inv_project_id, branch=branch)

        except Exception:
            try:
                await self.gitlab.delete_branch(project_id=self.inv_project_id, branch=branch)
            except Exception:
                self.logger.warning("Failed to cleanup branch=%s after error", branch)
            raise

        return eph

    # -----------------------------
    # Bulk scan primitives
    # -----------------------------

    async def _collect_all_instance_paths(self, *, zones: tuple[str, ...]) -> list[InstancePath]:
        paths: list[InstancePath] = []

        zone_env_dirs: list[str] = []
        for zone in zones:
            zone_tree = await self.gitlab.repository_tree(project_id=self.inv_project_id, path=zone, ref=self.inv_ref)
            zone_env_dirs.extend([i["path"] for i in zone_tree if i.get("mode") == "040000"])

        env_trees = await asyncio.gather(
            *(self.gitlab.repository_tree(project_id=self.inv_project_id, path=p, ref=self.inv_ref) for p in zone_env_dirs)
        )

        customer_dirs: list[str] = []
        for env_tree in env_trees:
            customer_dirs.extend([i["path"] for i in env_tree if i.get("mode") == "040000"])

        cust_trees = await asyncio.gather(
            *(self.gitlab.repository_tree(project_id=self.inv_project_id, path=p, ref=self.inv_ref) for p in customer_dirs)
        )

        for cust_tree in cust_trees:
            for inst_item in cust_tree:
                if inst_item.get("mode") != "040000":
                    continue
                ip = InstancePath.parse(inst_item["path"])
                paths.append(ip)
                self.cache.save_instance_path(ip)

        return paths

    async def _parse_instance(self, path: InstancePath) -> Instance:
        meta_path = f"{path.zone}/{path.env}/metadata.yaml"
        cust_path = f"{path.zone}/{path.env}/{path.cust_name}-{path.cust_apcode}/values.yaml"
        inst_values_path = f"{path.base_dir}/values.yaml"

        meta_file, cust_file, inst_file = await asyncio.gather(
            self.gitlab.get_file(project_id=self.inv_project_id, path=meta_path, ref=self.inv_ref),
            self.gitlab.get_file(project_id=self.inv_project_id, path=cust_path, ref=self.inv_ref),
            self.gitlab.get_file(project_id=self.inv_project_id, path=inst_values_path, ref=self.inv_ref),
        )

        if meta_file is None or inst_file is None:
            raise InvalidInventoryPathError(path.raw)

        meta = yaml.safe_load(meta_file.content) or {}
        cust_values = yaml.safe_load(cust_file.content) if cust_file else {}
        inst_values = yaml.safe_load(inst_file.content) or {}

        customer = Customer(
            apcode=path.cust_apcode,
            name=path.cust_name,
            snow_label=str(meta.get("customer") or path.cust_name),
        )

        cluster = Cluster(
            account=str(cust_values.get("IBM_ACCOUNT_ID") or cust_values.get("ibmAccountId") or ""),
            name=str(cust_values.get("IKS_CLUSTER_NAME") or cust_values.get("iksClusterName") or ""),
        )
        if cluster.name and not CLUSTER_REGEX.match(cluster.name):
            raise InvalidClusterNameError(cluster.name)

        env = path.env if path.env != "dhdev" else "dev"
        name = instance_name(path.apcode, env, path.release_id)
        url = url_from_name(inst_values, name)

        postgres = Service(
            host=str(inst_values.get("database", {}).get("host", f"{name}-pg")),
            port=int(inst_values.get("database", {}).get("port", 5432)),
        )
        elasticsearch = Service(
            host=str(inst_values.get("elasticsearch", {}).get("host", f"{name}-es")),
            port=int(inst_values.get("elasticsearch", {}).get("port", 9200)),
        )

        return Instance(
            apcode=path.apcode,
            cluster=cluster,
            customer=customer,
            database_secret_creation=is_database_secret_creation_enabled(inst_values),
            description=inst_values.get("description"),
            elasticsearch=elasticsearch,
            env=env,
            myaccess_creation=bool(inst_values.get("myaccessCreation", False)),
            name=name,
            postgres=postgres,
            release_id=path.release_id,
            source=Source(path=inst_values_path, project_id=self.inv_project_id, ref=self.inv_ref, values=inst_values),
            smtp=inst_values.get("airflow", {}).get("smtp"),
            url=url,
            version=str(meta.get("trigger", {}).get("branch") or meta.get("version") or ""),
            zone=path.zone,
            bucket_sync=inst_values.get("bucketSync"),
            git_sync=inst_values.get("gitSync"),
        )
