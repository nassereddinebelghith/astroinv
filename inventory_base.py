from aiohttp import ClientSession, ClientTimeout

import base64

from copy import deepcopy

from datetime import datetime

from enum import Enum

from jinja2 import Environment, Template, FileSystemLoader, TemplateNotFound

import logging

from pathlib import Path

from pydantic import BaseModel, Field, AfterValidator

import re

import requests

from requests import RequestException

import semver.version

import subprocess

from tenacity import retry, stop_after_attempt, wait_fixed

from time import sleep

from typing import Annotated

import urllib

import uuid

import yaml


from .constants import (
    APCODE_REGEX,
    CLUSTER_REGEX,
    EPH_FILENAME_REGEX,
    FIRST_VERSION,
    GITLAB_MAX_TRIES,
    GITLAB_TRY_WAIT,
    JINJA_ENV,
    LEGACY_PATH_REGEX,
    LEGACY_REF_BY_ENV,
    PATH_REGEX,
    VERSION_REGEX,
)
from .exceptions import (
    CustomerNotFoundError,
    FileNotFoundError,
    GitLabError,
    InstanceNotFoundError,
    InvalidClusterNameError,
    InvalidInstancePathError,
    InvalidInventoryPathError,
    InventoryError,
    SecretStoreNotFoundError,
)
from .models import *
from .cache import *

try:
    from .inventory_gitlab import GitLabMixin
except Exception:
    class GitLabMixin: pass
try:
    from .inventory_parse import ParseMixin
except Exception:
    class ParseMixin: pass
try:
    from .inventory_legacy import LegacyMixin
except Exception:
    class LegacyMixin: pass
try:
    from .inventory_ephemeral import EphemeralMixin
except Exception:
    class EphemeralMixin: pass
try:
    from .inventory_health import HealthMixin
except Exception:
    class HealthMixin: pass
try:
    from .inventory_query import QueryMixin
except Exception:
    class QueryMixin: pass
try:
    from .inventory_other import OtherMixin
except Exception:
    class OtherMixin: pass

class Inventory(GitLabMixin, ParseMixin, LegacyMixin, EphemeralMixin, HealthMixin, QueryMixin, OtherMixin):
    def init(

            self,

            gitlab_token: str,

            cache: Cache = InMemoryCache(),

            chart_project_id: int = 48413,

            cluster_project_id: int = 102423,

            cluster_ref: str = "main",

            gitlab_url: str = "https://gitlab-dogen.group.echonet/api/v4",

            inv_project_id: int = 87439,

            inv_ref: str = "main",

            legacy_inv_project_id: int = 44302,

    ):

        self.logger = logging.getLogger(__name__)

        self.gitlab_url = gitlab_url

        self.gitlab_token = gitlab_token

        self.cache = cache

        self.cluster_ref = cluster_ref

        self.inv_project_id = inv_project_id

        self.inv_ref = inv_ref

        self.legacy_inv_project_id = legacy_inv_project_id

        self.cluster_project_id = cluster_project_id

        self.chart_project_id = chart_project_id


    async def delete_ephemeral(

            self,

            release_id: str,

            number: int,

            author_name: str,

            author_email: str,

            no_merge: bool = False,

    ):

        eph = self.get_ephemeral(release_id, number)

        if eph is not None:

            now = datetime.now()

            branch = (

                f"{eph.instance.release_id}-{number}-{now.strftime('%Y%m%d%H%M%S')}"

            )

            await self._create_gitlab_branch(branch)

            await self._delete_gitlab_file(

                path=eph.source.path,

                ref=branch,

                msg=f"{eph.name}: delete",

                author_email=author_email,

                author_name=author_name,

            )

            mr_id = await self._create_gitlab_merge_request(

                branch, f"{eph.name}: delete"

            )

            if not no_merge:

                await self._merge_gitlab_merge_request(mr_id)


    async def get_all(

            self, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Instance]:

        insts = []

        for env in envs:

            env_insts = await self._get_all_from_env(env)

            release_ids = [inst.release_id for inst in env_insts]

            legacy_insts = await self._get_all_legacies_from_env(env, release_ids)

            insts.extend(env_insts)

            insts.extend(legacy_insts)

        return insts


    async def get_all_by_customer_apcode(

            self, cust_apcode: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Instance]:

        insts = []

        for env in envs:

            env_insts = await self._get_all_by_customer_apcode_from_env(

                env, cust_apcode

            )

            release_ids = [inst.release_id for inst in env_insts]

            legacy_insts = await self._get_all_legacies_by_customer_apcode_from_env(

                env, cust_apcode, release_ids

            )

            insts.extend(env_insts)

            insts.extend(legacy_insts)

        return insts

    async def get_all_by_customer_name(

            self, cust_name: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Instance]:

        insts = []

        for env in envs:

            env_insts = await self._get_all_by_customer_name_from_env(env, cust_name)

            release_ids = [inst.release_id for inst in env_insts]

            legacy_insts = await self._get_all_legacies_by_customer_name_from_env(

                env, cust_name, release_ids

            )

            insts.extend(env_insts)

            insts.extend(legacy_insts)

        return insts


    async def get_all_clusters_by_customer_apcode(

            self, cust_apcode: str, zones: list[str] = ["hprd", "prod"]

    ) -> list[Cluster]:

        clusters = []

        for zone in zones:

            self.logger.debug(f"listing clusters in {zone}")

            cluster_tree = await self._get_gitlab_repository_tree(

                self.cluster_project_id, zone, self.cluster_ref

            )

            for cluster_item in cluster_tree:

                if cluster_item["mode"] == "040000":

                    name_match = CLUSTER_REGEX.match(cluster_item["name"])

                    if name_match is not None and cust_apcode == name_match["apcode"]:

                        cluster_path = cluster_item["path"]

                        cluster = await self.__parse_cluster_if_workerpool_exists(

                            cluster_path

                        )

                        if cluster is not None:

                            clusters.append(cluster)

        return clusters


    async def get_all_ephemerals(

            self, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Ephemeral]:

        ephs = []

        for env in envs:

            env_ephs = await self._get_all_ephemerals_from_env(env)

            ephs.extend(env_ephs)

        return ephs


    async def get_all_ephemerals_by_customer_apcode(

            self, cust_apcode: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Ephemeral]:

        ephs = []

        for env in envs:

            env_ephs = await self._get_all_ephemerals_by_customer_apcode_from_env(

                env, cust_apcode

            )

            ephs.extend(env_ephs)

        return ephs


    async def get_all_ephemerals_by_customer_name(

            self, cust_name: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Ephemeral]:

        ephs = []

        for env in envs:

            env_ephs = await self._get_all_ephemerals_by_customer_name_from_env(

                env, cust_name

            )

            ephs.extend(env_ephs)

        return ephs


    async def get_all_ephemerals_by_release_id(

            self, release_id: str

    ) -> list[Ephemeral]:

        inst = await self.get_by_release_id(release_id)

        if inst is None:

            return []

        path = InstancePath.parse(inst.source.path)

        return await self._parse_all_ephemerals(path)


    async def get_all_health(

            self, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[AirflowHealth]:

        insts = await self.get_all(envs)

        return await self._get_all_health(insts)


    async def get_all_health_by_customer_apcode(

            self, cust_apcode: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[AirflowHealth]:

        insts = await self.get_all_by_customer_apcode(cust_apcode, envs)

        return await self._get_all_health(insts)


    async def get_all_health_by_customer_name(

            self, cust_name: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[AirflowHealth]:

        insts = await self.get_all_by_customer_name(cust_name, envs)

        return await self._get_all_health(insts)


    async def get_all_versions(self) -> list[str]:

        self.logger.debug("getting releases")

        releases = await self._get_gitlab_releases()

        versions = []

        for release in releases:

            version_match = VERSION_REGEX.match(release)

            if version_match is None:

                self.logger.debug(

                    f"version {release} ignored because it doesn't match version pattern"

                )

            else:

                version = semver.version.Version.parse(release)

                if version >= FIRST_VERSION:

                    versions.append(release)

                else:

                    self.logger.debug(

                        f"version {release} ignored because it's before {FIRST_VERSION}"

                    )

        return versions

    async def get_by_release_id(self, release_id: str) -> Instance | None:

        inst_path = await self._get_instance_path_by_release_id(release_id)

        if inst_path is None:

            return await self._get_legacy_by_release_id(release_id)

        else:

            return await self._parse_instance(inst_path)


    async def get_cluster_by_name(self, name: str) -> Cluster | None:

        cluster = self.cache.get_cluster(name)

        if cluster is None:

            cluster_match = CLUSTER_REGEX.match(name)

            if cluster_match is None:

                raise InvalidClusterNameError(name)

            zone = cluster_match["zone"]

            if zone == "pprd":

                zone = "prod"

            cluster_path = f"{zone}/{name}/cluster.yaml"

            self.logger.debug(

                f"reading {cluster_path} from clusters ({self.cluster_ref})"

            )

            try:

                cluster_file = await self._get_gitlab_file(

                    self.cluster_project_id, cluster_path, self.cluster_ref

                )

            except FileNotFoundError:

                return None

            self.logger.debug("parsing cluster metadata")

            cluster_meta = yaml.load(cluster_file.content, Loader=yaml.Loader)

            cluster = Cluster(

                account=cluster_meta["spec"]["ibmAccountRef"],

                name=name,

            )

            self.cache.save_cluster(cluster)

        return cluster


    async def get_customer_metadata(

            self, apcode: str, env: str

    ) -> CustomerMetadata | None:

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        for env in envs:

            path = f"{zone}/{env}"

            self.logger.debug(f"listing customers in {path}")

            cust_tree = await self._get_gitlab_repository_tree(

                self.inv_project_id, path, self.inv_ref

            )

            for cust_item in cust_tree:

                if cust_item["mode"] == "040000":

                    cust_path = cust_item["path"]

                    suffix = f"-{apcode}"

                    if cust_path.endswith(suffix):

                        cust_name = cust_path.removeprefix(f"{path}/").removesuffix(

                            suffix

                        )

                        values_path = f"{cust_path}/values.yaml"

                        self.logger.debug(

                            f"reading {values_path} from inventory ({self.inv_ref})"

                        )

                        values_content_file = await self._get_gitlab_file(

                            self.inv_project_id, values_path, self.inv_ref

                        )

                        self.logger.debug("parsing customer values")

                        values = yaml.load(

                            values_content_file.content, Loader=yaml.Loader

                        )

                        return self.__parse_customer_metadata(cust_name, apcode, values)

        return None


    async def get_ephemeral(self, release_id: str, number: int) -> Ephemeral | None:

        inst = await self._get_ephemeral_from_zone("hprd", release_id, number)

        if inst is not None:

            return inst

        return await self._get_ephemeral_from_zone("prod", release_id, number)


    async def save(

            self,

            cfg: InstanceConfig,

            author_name: str,

            author_email: str,

            dry_run: bool = False,

            no_merge: bool = False,

    ) -> Instance:

        inst_name = instance_name(cfg.apcode, cfg.env, cfg.release_id)

        zone = zone_from_env(cfg.env)

        cust_meta = await self.get_customer_metadata(cfg.customer_apcode, cfg.env)

        if cust_meta is None:

            raise CustomerNotFoundError(cfg.apcode, cfg.env)

        cluster = await self.get_cluster_by_name(cfg.cluster_name)

        if cluster is None:

            raise ClusterNotFoundError(cfg.cluster_name)

        env = cfg.env

        if env == "dev" and cust_meta.customer.name == "datahub":

            env = "dhdev"

        inst_path = InstancePath(

            apcode=cfg.apcode,

            customer_apcode=cust_meta.customer.apcode,

            customer_name=cust_meta.customer.name,

            env=env,

            path=f"{zone}/{env}/{cust_meta.customer.name}-{cust_meta.customer.apcode}/{cfg.apcode}-{cfg.release_id}",

            release_id=cfg.release_id,

            zone=zone,

        )

        meta_path = f"{inst_path.path}/metadata.yaml"

        values_path = f"{inst_path.path}/values.yaml"

        meta_file = await self._get_gitlab_file(

            self.inv_project_id, meta_path, self.inv_ref

        )

        values_file = await self._get_gitlab_file(

            self.inv_project_id, values_path, self.inv_ref

        )

        if values_file is None:

            self.logger.debug(f"{values_path} not found, seems to be a new instance")

        else:

            self.logger.debug("parsing values")

            old_values = yaml.load(values_file.content, Loader=yaml.Loader)

        inst = Instance(

            apcode=cfg.apcode,

            bucket_sync=cfg.bucket_sync,

            cluster=cluster,

            customer=cust_meta.customer,

            database_secret_creation=cfg.database_secret_creation,

            description=cfg.description,

            elasticsearch=cust_meta.elasticsearch,

            env=cfg.env,

            git_sync=cfg.git_sync,

            myaccess_creation=cfg.myaccess_creation,

            name=inst_name,

            postgres=cust_meta.postgres,

            release_id=cfg.release_id,

            source=Source(

                path=inst_path.path,

                project_id=self.inv_project_id,

                ref=self.inv_ref,

            ),

            smtp=cfg.smtp,

            url=url_from_name(inst_name),

            version=cfg.version,

            zone=zone,

        )

        now = datetime.now()

        branch = f"{inst.release_id}-{now.strftime('%Y%m%d%H%M%S')}"

        meta_tpl = self._parse_template("metadata")

        meta_content = meta_tpl.render(inst=inst)

        version_match = VERSION_REGEX.match(cfg.version)

        if version_match is None:

            values_tpl = self._parse_template("values-latest")

        else:

            version = semver.version.Version.parse(cfg.version)

            tpl_name = f"values-{version.major}.{version.minor}"

            try:

                values_tpl = self._parse_template(tpl_name)

            except TemplateNotFound as err:

                self.logger.warn(f"template {tpl_name} doesn't exist, using latest")

                values_tpl = self._parse_template("values-latest")

        secrets = [sync.secret for sync in inst.bucket_sync]

        secrets.extend([sync.secret for sync in inst.git_sync])

        if inst.smtp is not None:

            secrets.append(inst.smtp.secret)

        values_content = values_tpl.render(

            inst=inst,

            secret_stores=self._secret_stores_from_secrets(secrets),

        )

        inst.source.metadata = yaml.load(meta_content, Loader=yaml.Loader)

        inst.source.values = yaml.load(values_content, Loader=yaml.Loader)

        meta_content = yaml.dump(inst.source.metadata)

        values_content = yaml.dump(inst.source.values)

        if not dry_run:

            await self._create_gitlab_branch(branch)

            changed = False

            if meta_file is None:

                mr_name = f"{inst.name}: create"

                await self._create_gitlab_file(

                    path=meta_path,

                    content=meta_content,

                    ref=branch,

                    msg=f"{inst.name}: add metadata",

                    author_name=author_name,

                    author_email=author_email,

                )

                changed = True

            else:

                mr_name = f"{inst.name}: update"

                updated = await self._update_gitlab_file(

                    file=meta_file,

                    content=meta_content,

                    ref=branch,

                    msg=f"{inst.name}: update metadata",

                    author_name=author_name,

                    author_email=author_email,

                )

                if updated:

                    changed = True

            if values_file is None:

                await self._create_gitlab_file(

                    path=values_path,

                    content=values_content,

                    ref=branch,

                    msg=f"{inst.name}: add values",

                    author_name=author_name,

                    author_email=author_email,

                )

                changed = True

            else:

                updated = await self._update_gitlab_file(

                    file=values_file,

                    content=values_content,

                    ref=branch,

                    msg=f"{inst.name}: update values",

                    author_name=author_name,

                    author_email=author_email,

                )

                if updated:

                    changed = True

            if changed:

                mr_id = await self._create_gitlab_merge_request(branch, mr_name)

                if not no_merge:

                    await self._merge_gitlab_merge_request(mr_id)

            else:

                await self._delete_gitlab_branch(branch)

        self.cache.save_instance_path(inst_path)

        return inst

    async def save_ephemeral(

            self,

            cfg: EphemeralConfig,

            author_name: str,

            author_email: str,

            dry_run: bool = False,

            no_merge: bool = False,

    ) -> Ephemeral:

        inst = self.get_by_release_id(cfg.source)

        if inst is None:

            raise InstanceNotFoundError(cfg.source)

        env = inst.env

        if env == "dev" and inst.customer.name == "datahub":

            env = "dhdev"

        eph_name = ephemeral_name(inst.name, cfg.number)

        values_path = f"{inst.zone}/{env}/{inst.customer.name}-{inst.customer.apcode}/{inst.apcode}-{inst.release_id}/eph{cfg.number}.yaml"

        eph = Ephemeral(

            bucket_sync=cfg.bucket_sync,

            git_sync=cfg.git_sync,

            instance=inst,

            name=eph_name,

            number=cfg.number,

            source=Source(

                path=values_path,

                project_id=self.inv_project_id,

                ref=self.inv_ref,

            ),

        )

        now = datetime.now()

        branch = f"{inst.release_id}-{cfg.number}-{now.strftime('%Y%m%d%H%M%S')}"

        secrets = [sync.secret for sync in cfg.bucket_sync]

        secrets.extend([sync.secret for sync in cfg.git_sync])

        values_tpl = self._parse_template("eph")

        values_content = values_tpl.render(

            eph=eph,

            secret_stores=self._secret_stores_from_secrets(secrets),

        )

        eph.values = yaml.load(values_content, Loader=yaml.Loader)

        values_content = yaml.dump(eph.values)

        if not dry_run:

            await self._create_gitlab_branch(branch)

            values_file = await self._get_gitlab_file(

                self.inv_project_id, values_path, branch

            )

            changed = False

            if values_file is None:

                mr_name = f"{eph.name}: create"

                await self._create_gitlab_file(

                    path=values_path,

                    content=values_content,

                    ref=branch,

                    msg=f"{eph.name}: add values",

                    author_name=author_name,

                    author_email=author_email,

                )

                changed = True

            else:

                mr_name = f"{eph.name}: update"

                updated = await self._update_gitlab_file(

                    path=values_file,

                    content=values_content,

                    ref=branch,

                    msg=f"{eph.name}: update values",

                    author_name=author_name,

                    author_email=author_email,

                )

                if updated:

                    changed = True

            if changed:

                mr_id = await self._create_gitlab_merge_request(branch, mr_name)

                if not no_merge:

                    await self._merge_gitlab_merge_request(mr_id)

            else:

                await self._delete_gitlab_branch(branch)

        return eph

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))


