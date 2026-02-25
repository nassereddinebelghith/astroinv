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


class ParseMixin:
    async def _parse_all_ephemerals(self, path: InstancePath) -> list[Ephemeral]:

        ephs = []

        tree = await self._get_gitlab_repository_tree(

            self.inv_project_id, path.path, self.inv_ref

        )

        for item in tree:

            name_match = EPH_FILENAME_REGEX.match(item["name"])

            if name_match is not None:

                number = name_match["number"]

                eph_file = await self._get_gitlab_file(

                    self.inv_project_id, item["path"], self.inv_ref

                )

                eph = await self.__parse_ephemeral(path, number, eph_file)

                ephs.append(eph)

        return ephs


    def _parse_bucket_sync(self, inst_values: dict) -> list[BucketSync]:

        mapping = BucketSyncSecretMapping()

        global_bucket_sync = inst_values.get("bucketSync")

        if global_bucket_sync is not None:

            global_ext_secret = global_bucket_sync.get("externalSecret")

            if global_ext_secret is not None:

                global_props = global_ext_secret.get("defaultProperty")

                mapping = self._parse_bucket_sync_mapping(global_props)

        bucket_sync = []

        secret_stores = inst_values.get("extraSecretStores", [])

        for bucket_dict in inst_values.get("buckets", []):

            ext_secret = bucket_dict["externalSecret"]

            secret = self._parse_secret(secret_stores, ext_secret)

            bucket_props = ext_secret.get("property")

            bucket_sync.append(

                BucketSync(

                    bucket=bucket_dict["name"],

                    dags_path=bucket_dict.get("dagsPath", "/"),

                    mapping=self._parse_bucket_sync_mapping(bucket_props, mapping),

                    secret=secret,

                    target_dir=bucket_dict.get("targetDir"),

                )

            )

        return bucket_sync

    def _parse_bucket_sync_mapping(

            self,

            props: dict | None,

            default: BucketSyncSecretMapping = BucketSyncSecretMapping(),

    ) -> BucketSyncSecretMapping:

        mapping = deepcopy(default)

        if props is not None:

            access_key_id = props.get("accessKeyId")

            secret_access_key = props.get("secretAccessKey")

            if access_key_id is not None:

                mapping.access_key_id = access_key_id

            if secret_access_key is not None:

                mapping.secret_access_key = secret_access_key

        return mapping
async def __parse_cluster_if_workerpool_exists(

        self, cluster_path: str

) -> Cluster | None:

    self.logger.debug(f"listing worker pools in {cluster_path}")

    wp_tree = await self._get_gitlab_repository_tree(

        self.cluster_project_id, cluster_path, self.cluster_ref

    )

    for wp_item in wp_tree:

        if wp_item["name"] == "workerpoolclaims.yaml":

            wp_path = wp_item["path"]

            self.logger.debug(f"reading {wp_path}")

            wp_content_file = await self._get_gitlab_file(

                self.cluster_project_id, wp_path, self.cluster_ref

            )

            self.logger.debug(f"parsing {wp_path}")

            wps = yaml.load_all(wp_content_file.content, Loader=yaml.Loader)

            for wp in wps:

                wp_spec = wp["spec"]

                if "labels" in wp_spec:

                    wp_labels = wp_spec["labels"]

                    if "reservation.data.itgp.bnp/product" in wp_labels:

                        product = wp_labels["reservation.data.itgp.bnp/product"]

                        if product == "astronomer":

                            return await self.get_cluster_by_name(

                                wp_spec["clusterRef"]

                            )

    return None
def __parse_customer_metadata(

        self, name: str, apcode: str, values: dict

) -> CustomerMetadata:

    return CustomerMetadata(

        customer=Customer(

            apcode=apcode,

            name=name,

            snow_label=values["customer"],

        ),

        elasticsearch=Service(

            host=values["elasticsearch"]["host"],

            port=values["elasticsearch"]["port"],

        ),

        postgres=Service(

            host=values["database"]["host"],

            port=values["database"]["port"],

        ),

    )

async def __parse_ephemeral(

        self, path: InstancePath, number: int, eph_file: GitLabFile

) -> Ephemeral:

    inst = await self._parse_instance(path)

    values = yaml.load(eph_file.content, Loader=yaml.Loader)

    return Ephemeral(

        bucket_sync=self._parse_bucket_sync(values),

        git_sync=self._parse_git_sync(values),

        instance=inst,

        name=ephemeral_name(inst.name, number),

        number=number,

        source=Source(

            path=eph_file.path,

            project_id=self.inv_project_id,

            ref=self.inv_ref,

        ),

        values=values,

    )

    def _parse_git_sync(self, inst_values: dict) -> list[GitSync]:

        mapping = GitSyncSecretMapping()

        global_git_sync = inst_values.get("gitSync")

        if global_git_sync is not None:

            global_ext_secret = global_git_sync.get("externalSecret")

            if global_ext_secret is not None:

                global_props = global_ext_secret.get("defaultProperty")

                mapping = self._parse_git_sync_mapping(global_props)

        git_sync = []

        secret_stores = inst_values.get("extraSecretStores", [])

        for repo_dict in inst_values.get("gitRepositories", []):

            ext_secret = repo_dict["externalSecret"]

            props = ext_secret.get("property")

            git_sync.append(

                GitSync(

                    branch=repo_dict.get("branch"),

                    mapping=self._parse_git_sync_mapping(props, mapping),

                    secret=self._parse_secret(secret_stores, ext_secret),

                    target_dir=repo_dict.get("targetDir"),

                )

            )

        return git_sync

    def _parse_git_sync_mapping(

            self, props: dict | None, default: GitSyncSecretMapping = GitSyncSecretMapping()

    ) -> GitSyncSecretMapping:

        mapping = deepcopy(default)

        if props is not None:

            branch = props.get("branch")

            password = props.get("password")

            url = props.get("url")

            user = props.get("user")

            if branch is not None:

                mapping.branch = branch

            if password is not None:

                mapping.password = password

            if url is not None:

                mapping.url = url

            if user is not None:

                mapping.user = user

        return mapping

    async def _parse_instance(self, path: InstancePath) -> Instance:

        real_env = path.env

        if real_env == "dhdev":

            real_env = "dev"

        inst_name = instance_name(path.apcode, real_env, path.release_id)

        meta_path = f"{path.path}/metadata.yaml"

        self.logger.debug(f"reading {meta_path} from inventory ({self.inv_ref})")

        meta_file = await self._get_gitlab_file(

            self.inv_project_id, meta_path, self.inv_ref

        )

        cust_values_path = f"{path.zone}/{path.env}/{path.customer_name}-{path.customer_apcode}/values.yaml"

        self.logger.debug(f"reading {cust_values_path} from inventory ({self.inv_ref})")

        cust_values_file = await self._get_gitlab_file(

            self.inv_project_id, cust_values_path, self.inv_ref

        )

        inst_values_path = f"{path.path}/values.yaml"

        self.logger.debug(f"reading {inst_values_path} from inventory ({self.inv_ref})")

        inst_values_file = await self._get_gitlab_file(

            self.inv_project_id, inst_values_path, self.inv_ref

        )

        self.logger.debug("parsing metadata")

        meta = yaml.load(meta_file.content, Loader=yaml.Loader)

        self.logger.debug("parsing customer values")

        cust_values = yaml.load(cust_values_file.content, Loader=yaml.Loader)

        self.logger.debug("parsing instance values")

        inst_values = yaml.load(inst_values_file.content, Loader=yaml.Loader)

        cluster = await self.get_cluster_by_name(meta["cluster"])

        cust_meta = self.__parse_customer_metadata(

            path.customer_name, path.customer_apcode, cust_values

        )

        myaccess_creation = is_myaccess_creation_enabled(inst_values)

        database_secret_creation = is_database_secret_creation_enabled(inst_values)

        inst = Instance(

            apcode=path.apcode,

            bucket_sync=self._parse_bucket_sync(inst_values),

            cluster=cluster,

            customer=cust_meta.customer,

            database_secret_creation=database_secret_creation,

            description=meta.get("description"),

            elasticsearch=cust_meta.elasticsearch,

            env=real_env,

            git_sync=self._parse_git_sync(inst_values),

            myaccess_creation=myaccess_creation,

            name=inst_name,

            postgres=cust_meta.postgres,

            release_id=path.release_id,

            smtp=self._parse_smtp(inst_values),

            source=Source(

                metadata=meta,

                path=path.path,

                project_id=self.inv_project_id,

                ref=self.inv_ref,

                values=inst_values,

            ),

            url=url_from_name(inst_name),

            version=meta["version"],

            zone=path.zone,

        )

        self.cache.save_instance_path(path)

        return inst

    async def _parse_legacy_instances(self, path: str, env: str) -> list[Instance]:

        insts = []

        path_match = LEGACY_PATH_REGEX.match(path)

        if path_match is None:

            raise InvalidInventoryPathError(path)

        self.logger.debug(

            f"reading {path} from legacy inventory ({LEGACY_REF_BY_ENV[env]})"

        )

        inv_file = await self._get_gitlab_file(

            self.legacy_inv_project_id, path, LEGACY_REF_BY_ENV[env]

        )

        self.logger.debug("parsing legacy inventory")

        inv_dicts = yaml.load(inv_file.content, Loader=yaml.Loader)

        for inv_dict in inv_dicts.values():

            inv_vars = inv_dict["variables"]

            self.logger.debug("parsing custom values")

            inst_values = yaml.load(inv_vars["CUSTOM_VALUES"], Loader=yaml.Loader)

            cluster = Cluster(

                account=inv_vars["IBM_ACCOUNT_ID"],

                name=inv_vars["IKS_CLUSTER_NAME"],

            )

            cluster_match = CLUSTER_REGEX.match(cluster.name)

            if cluster_match is None:

                raise InvalidClusterNameError(cluster.name)

            apcode = inv_vars["APP_CODE"]

            release_id = inv_vars["RELEASE_UUID"]

            inst_name = instance_name(apcode, env, release_id)

            pg_name = inv_vars["PG_INSTANCE"].lower()

            es_name = inv_vars["ES_INSTANCE"].lower()

            zone = zone_from_env(env)

            if zone == "prod":

                pg_host = f"{pg_name}.svc.paas.echonet"

                es_host = f"{es_name}.svc.paas.echonet"

            else:

                pg_host = f"{pg_name}.svc-np.paas.echonet"

                es_host = f"{es_name}.svc-np.paas.echonet"

            if "ES_PORT" in inv_vars:

                es_port = int(inv_vars["ES_PORT"])

            else:

                es_port = int(inst_values["elasticsearch"]["port"])

            database_secret_creation = is_database_secret_creation_enabled(inst_values)

            insts.append(

                Instance(

                    apcode=apcode,

                    bucket_sync=self._parse_bucket_sync(inst_values),

                    cluster=cluster,

                    customer=Customer(

                        apcode=cluster_match["apcode"],

                        name=path_match["cust"],

                        snow_label=inv_vars["CLIENT_NAME"],

                    ),

                    database_secret_creation=database_secret_creation,

                    description=inv_vars.get("DESCRIPTION"),

                    elasticsearch=Service(

                        host=es_host,

                        port=es_port,

                    ),

                    env=env,

                    git_sync=self._parse_git_sync(inst_values),

                    myaccess_creation=False,

                    name=inst_name,

                    postgres=Service(

                        host=pg_host,

                        port=int(inv_vars["PG_PORT"]),

                    ),

                    release_id=str(release_id),

                    source=Source(

                        path=path,

                        project_id=self.legacy_inv_project_id,

                        ref=LEGACY_REF_BY_ENV[env],

                        values=inst_values,

                    ),

                    smtp=self._parse_smtp(inst_values),

                    url=url_from_name(inst_name),

                    version=inv_dict["trigger"]["branch"],

                    zone=zone,

                )

            )

        return insts

    def _parse_secret(self, secret_stores: list[dict], ext_secret: dict) -> Secret:

        secret_store_name = ext_secret["secretStore"]

        secret_store = next(

            (

                secret_store

                for secret_store in secret_stores

                if secret_store["name"] == secret_store_name

            ),

            None,

        )

        if secret_store is None:

            raise SecretStoreNotFoundError(secret_store_name)

        return Secret(

            engine=secret_store["path"],

            namespace=secret_store["namespace"],

            path=ext_secret["path"],

            url=secret_store["server"],

        )


    def _parse_smtp(self, inst_values: dict) -> Smtp | None:

        if "airflow" not in inst_values:

            return None

        airflow_values = inst_values["airflow"]

        if "smtp" not in airflow_values:

            return None

        smtp_values = airflow_values["smtp"]

        if "enabled" not in smtp_values or not smtp_values["enabled"]:

            return None

        secret_stores = inst_values["extraSecretStores"]

        ext_secret = smtp_values["externalSecret"]

        secret = self._parse_secret(secret_stores, ext_secret)

        props = ext_secret.get("property")

        mapping = SmtpSecretMapping()

        if props is not None:

            host = props.get("host")

            password = props.get("password")

            port = props.get("port")

            sender = props.get("sender")

            user = props.get("user")

            if host is not None:

                mapping.host = host

            if password is not None:

                mapping.password = password

            if port is not None:

                mapping.port = port

            if sender is not None:

                mapping.sender = sender

            if user is not None:

                mapping.user = user

        return Smtp(

            mapping=mapping,

            secret=secret,

        )


    def _parse_template(self, name: str) -> Template:

        self.logger.debug(f"parsing template {name}")

        return JINJA_ENV.get_template(f"{name}.j2")


    def _secret_stores_from_secrets(

            self, secrets: list[Secret]

    ) -> dict[str, SecretStore]:

        secret_stores = {}

        for secret in secrets:

            secret_store_name = secret.secret_store_name()

            if not secret_store_name in secret_stores:

                secret_stores[secret_store_name] = SecretStore(

                    namespace=secret.namespace,

                    path=secret.engine,

                    server=secret.url,

                )

        return secret_stores
    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

