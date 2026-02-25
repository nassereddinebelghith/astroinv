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

APCODE_REGEX = re.compile(r"^a(p|[0-9])[0-9]{5}$")

CLUSTER_REGEX = re.compile(r"^iks-(?P<apcode>[a-z0-9]+)-(?P<zone>[a-z0-9]+)-[a-z0-9]+$")

EPH_FILENAME_REGEX = re.compile(r"eph(?P<number>[0-9]+)\.yaml$")

LEGACY_REF_BY_ENV = {

    "dev": "dev",

    "int": "int",

    "qual": "qualif",

    "pprd": "pprd",

    "prod": "prod",

}

PATH_REGEX = re.compile(

    r"^(?P<zone>prod|hprd)/(?P<env>[a-z0-9]+)/(?P<cust>[a-z0-9]+)-(?P<cust_apcode>[a-z0-9]+)/(?P<apcode>[a-z0-9]+)-(?P<release_id>[a-z0-9]+)$"

)

LEGACY_PATH_REGEX = re.compile(r"^config/(?P<cust>[a-z0-9]+)\.ya?ml$")

VERSION_REGEX = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")

TEMPLATES_PATH = Path(__file__).parent.joinpath(f"templates")

JINJA_ENV = Environment(

    loader=FileSystemLoader(TEMPLATES_PATH),

    keep_trailing_newline=True,

)

GITLAB_TRY_WAIT = 5

GITLAB_MAX_TRIES = 10

FIRST_VERSION = semver.version.Version(major=3, minor=4, patch=0)

class InventoryError(Exception):

    pass

class GitLabError(InventoryError):

    def init(self, status: int):

        super().__init__(f"GitLab returned an error {status}")

class InvalidClusterNameError(InventoryError):

    def init(self, cluster_name: str):

        super().__init__(f"{cluster_name} is not a valid cluster name")

class InvalidInstancePathError(InventoryError):

    def init(self, path: str):

        super().__init__(f"{path} is not a valid instance path")

class InvalidInventoryPathError(InventoryError):

    def init(self, path: str):

        super().__init__(f"{path} is not a valid inventory path")

class FileNotFoundError(InventoryError):

    def init(self, path: str):

        super().__init__(f"{path} doesn't exist")

class SecretStoreNotFoundError(InventoryError):

    def init(self, secret_store: str):

        super().__init__(f"Secret store {secret_store} doesn't exist")

class CustomerNotFoundError(InventoryError):

    def init(self, apcode: str, env: str):

        super().__init__(f"Customer {apcode} does not exist in environment {env}")

class InstanceNotFoundError(InventoryError):

    def init(self, release_id: str):

        super().__init__(f"Instance {release_id} doesn't exist")

def ephemeral_name(inst_name: str, number: int) -> str:

    return f"{inst_name}-eph{number}"

def generate_release_id() -> str:

    release_uuid = str(uuid.uuid4())

    return release_uuid.split("-")[0]

def instance_name(apcode: str, env: str, release_id: str) -> str:

    return f"astronomer-{apcode}-{env}-{release_id}"

def is_database_secret_creation_enabled(inst_values: dict) -> bool:

    database_secret_creation = False

    database = inst_values.get("database")

    if database is not None:

        database_secret = database.get("secret")

        if database_secret is not None:

            database_secret_creation = database_secret.get("create", False)

    return database_secret_creation

def is_myaccess_creation_enabled(inst_values: dict) -> bool:

    myaccess_creation = True

    myaccess = inst_values.get("myaccess")

    if myaccess is not None:

        myaccess_creation = myaccess.get("create", True)

    return myaccess_creation

def is_strictly_positive(number: int) -> int:

    if number <= 0:

        raise ValueError(f"{number} should be > 0")

    return number

def is_valid_apcode(apcode: str) -> str:

    if APCODE_REGEX.match(apcode) is None:

        raise ValueError(f"{apcode} is not a valid AP Code")

    return apcode

def is_valid_cluster_name(name: str) -> str:

    if CLUSTER_REGEX.match(name) is None:

        raise ValueError(f"{name} is not a valid cluster name")

    return name

def is_valid_env(env: str) -> str:

    if env not in LEGACY_REF_BY_ENV:

        raise ValueError(f"{env} is not a valid environment")

    return env

def url_from_name(name: str) -> str:

    return f"https://{name}.data.cloud.net.intra"

def zone_from_env(env: str) -> str:

    if env == "prod" or env == "pprd":

        return "prod"

    else:

        return "hprd"

class GitLabFile(BaseModel):

    content: str

    path: str

class Secret(BaseModel):

    """

    A Vault secret

    """

    engine: Annotated[str, Field(description="Vault secret engine")]

    namespace: Annotated[str, Field(description="Vault namespace")]

    path: Annotated[str, Field(description="Path to secret in engine")]

    url: Annotated[str, Field(description="Vault base URL")]

    def hash(self):

        return hash((self.url, self.namespace, self.engine, self.path))

    def secret_store_name(self) -> str:

        ec = self.namespace.split("/")[-1]

        return f"{ec}-{self.engine}".lower()

class SecretStore(BaseModel):

    namespace: str

    path: str

    server: str

class Sync(BaseModel):

    secret: Annotated[Secret, Field(description="Secret used for synchronization")]

    target_dir: Annotated[

        str | None,

        Field(description="Path to DAGs directory"),

    ] = None

class GitSyncSecretMapping(BaseModel):

    """

    Secrets mapping for git synchronization

    """

    branch: Annotated[

        str,

        Field(description="Name of the property of the secret containing the branch"),

    ] = "branch"

    password: Annotated[

        str,

        Field(description="Name of the property of the secret containing the pasword"),

    ] = "password"

    url: Annotated[

        str,

        Field(

            description="Name of the property of the secret containing repository URL"

        ),

    ] = "repo"

    user: Annotated[

        str,

        Field(

            description="The name of the property of the secret that contains usrname"

        ),

    ] = "user"

class BucketSyncSecretMapping(BaseModel):

    """

    Secret mapping for bucket synchronization

    """

    access_key_id: Annotated[

        str,

        Field(

            description="The name of the property of the secret that contains access key ID"

        ),

    ] = "cos_hmac_keys_access_key_id"

    secret_access_key: Annotated[

        str,

        Field(

            description="Name of the property of the secret containing secret access key"

        ),

    ] = "cos_hmac_keys_secret_access_key"

class GitSync(Sync):

    """

    Git synchronization

    """

    branch: Annotated[

        str | None,

        Field(

            description="Branch used for synchronizing (overrides the branch in the Vault secret)"

        ),

    ] = None

    mapping: Annotated[

        GitSyncSecretMapping, Field(description="Secret property mapping")

    ] = GitSyncSecretMapping()

class BucketSync(Sync):

    """

    A bucket synchronization

    """

    bucket: Annotated[str, Field(description="Bucket name")]

    dags_path: Annotated[str, Field(description="Path to the dags in the bucket")]

    mapping: Annotated[

        BucketSyncSecretMapping, Field(description="Secret property mapping")

    ] = BucketSyncSecretMapping()

class Cluster(BaseModel):

    """

    A cluster

    """

    account: Annotated[str, Field(description="IBM account")]

    name: Annotated[str, Field(description="IKS cluster name")]

class Customer(BaseModel):

    """

    A customer

    """

    apcode: Annotated[str, Field(description="Customer APCODE")]

    name: str = Annotated[str, Field(description="Customer name")]

    snow_label: str = Annotated[

        str, Field(description="Label to reference customers in MyAccess rights")

    ]

class Service(BaseModel):

    """

    A service

    """

    host: Annotated[str, Field(description="The host of the service")]

    port: Annotated[int, Field(description="The port of the service")]

class Source(BaseModel):

    """

    Instance inventory source

    """

    metadata: Annotated[dict, Field(description="Raw instance metadata")] = {}

    path: Annotated[

        str, Field(description="Path to the directory/file describing the instance")

    ]

    project_id: Annotated[

        int,

        Field(description="ID of GitLab project containing the instance definition"),

    ]

    ref: Annotated[

        str,

        Field(

            description="GitLab reference of the project containing the instance definition"

        ),

    ]

    values: Annotated[dict, Field(description="Raw values for the instance")] = {}
class SmtpSecretMapping(BaseModel):

    """

    Secret mapping used for SMTP configuration

    """

    host: Annotated[

        str,

        Field(description="Name of the property of the host secret containing host"),

    ] = "host"

    password: Annotated[

        str,

        Field(description="Name of the property of the password secret"),

    ] = "password"

    port: Annotated[

        str,

        Field(description="Name of the property of the port secret"),

    ] = "port"

    sender: Annotated[

        str,

        Field(description="Name of the property of the sender secret"),

    ] = "sender"

    user: Annotated[

        str,

        Field(description="The name of the property of the secret that contains user"),

    ] = "user"

class Smtp(BaseModel):

    """

    SMTP configuration

    """

    mapping: Annotated[

        SmtpSecretMapping, Field(description="Secret property mapping")

    ] = SmtpSecretMapping()

    secret: Annotated[Secret, Field(description="Secret for SMTP configuration")]

class InstanceConfig(BaseModel):

    """

    Instance configuration

    """

    apcode: Annotated[

        str, Field(description="Instance APCODE"), AfterValidator(is_valid_apcode)

    ]

    bucket_sync: Annotated[

        list[BucketSync], Field(description="List of bucket synchronization")

    ] = []

    cluster_name: Annotated[

        str,

        Field(description="IKS cluster name"),

        AfterValidator(is_valid_cluster_name),

    ]

    customer_apcode: Annotated[

        str, Field(description="Customer APCODE"), AfterValidator(is_valid_apcode)

    ]

    database_secret_creation: Annotated[

        bool,

        Field(

            description="True if database secret must be generated at provisioning, false if it already exists"

        ),

    ] = True

    description: Annotated[

        str | None, Field(description="Description of the instance")

    ] = None

    env: Annotated[

        str,

        Field(description="Environment of the instance"),

        AfterValidator(is_valid_env),

    ]

    git_sync: Annotated[

        list[GitSync], Field(description="List of git synchronization")

    ] = []

    myaccess_creation: Annotated[

        bool,

        Field(

            description="True if NyAccess rights/teams must be created at provisioning, otherwise false"

        ),

    ] = True

    release_id: Annotated[

        str,

        Field(

            description="Instance release ID",

            default_factory=generate_release_id,

        ),

    ]

    smtp: Annotated[Smtp | None, Field(description="SMTP configuration")] = None

    version: Annotated[str, Field(description="OaaS version to use")]

class Instance(BaseModel):

    """

    Instance definition

    """

    apcode: Annotated[str, Field(description="Instance APCODE")]

    bucket_sync: Annotated[

        list[BucketSync], Field(description="List of bucket synchronization")

    ] = []

    cluster: Annotated[Cluster, Field(description="Cluster where the instance resides")]

    customer: Annotated[Customer, Field(description="Customer that owns the instance")]

    database_secret_creation: Annotated[

        bool,

        Field(

            description="True if database secret must be generated at provisioning, false if it already exists"

        ),

    ] = True

    description: Annotated[

        str | None, Field(description="Description of the instance")

    ] = None

    elasticsearch: Annotated[

        Service, Field(description="Elasticsearch used by the instance")

    ]

    env: Annotated[str, Field(description="Environment of the instance")]

    git_sync: Annotated[

        list[GitSync], Field(description="List of git synchronization")

    ] = []

    myaccess_creation: Annotated[

        bool,

        Field(

            description="True if NyAccess rights/teams must be created at provisioning, false otherwise"

        ),

    ] = True

    name: Annotated[str, Field(description="Instance name")]

    postgres: Annotated[Service, Field(description="PostgreSQL used by the instance")]

    release_id: Annotated[str, Field(description="Instance release ID")]

    source: Annotated[Source, Field(description="Source describing the instance")]

    smtp: Annotated[Smtp | None, Field(description="SMTP configuration")] = None

    url: Annotated[str, Field(description="Instance URL")]

    version: Annotated[str, Field(description="OaaS version to use")]

    zone: Annotated[str, Field(description="Environment zone")]

    def config(self) -> InstanceConfig:

        return InstanceConfig(

            apcode=self.apcode,

            bucket_sync=self.bucket_sync,

            cluster_name=self.cluster.name,

            customer_apcode=self.customer.apcode,

            database_secret_creation=is_database_secret_creation_enabled(

                self.source.values

            ),

            description=self.description,

            env=self.env,

            git_sync=self.git_sync,

            myaccess_creation=is_myaccess_creation_enabled(self.source.values),

            release_id=self.release_id,

            smtp=self.smtp,

            version=self.version,

        )

class EphemeralConfig(BaseModel):

    """

    Ephemeral instance configuration

    """

    bucket_sync: Annotated[

        list[BucketSync],

        Field(description="List of bucket synchronization, overrides source instance"),

    ] = []

    git_sync: Annotated[

        list[GitSync],

        Field(description="Git synchronization, overrides source instance"),

    ] = []

    number: Annotated[

        int,

        Field(description="Unique ID of the ephemeral instance"),

        is_strictly_positive,

    ]

    source: Annotated[str, Field(description="Source instance name")]

class Ephemeral(BaseModel):

    """

    Ephemeral instance definition

    """

    bucket_sync: Annotated[

        list[BucketSync],

        Field(description="List of bucket synchronization, overrides source instance"),

    ] = []

    git_sync: Annotated[

        list[GitSync],

        Field(description="List of git synchronization, overrides source instance"),

    ] = []

    instance: Annotated[Instance, Field(description="Source instance")]

    name: Annotated[str, Field(description="Name of ephemeral instance")]

    number: Annotated[int, Field(description="Unique ID of the ephemeral instance")]

    source: Annotated[

        Source, Field(description="Source that describes the ephemeral instance")

    ]

    values: Annotated[

        dict, Field(description="Custom values of the ephemeral instance")

    ] = {}

class CustomerMetadata(BaseModel):

    """

    Customer metadata

    """

    customer: Annotated[Customer, Field(description="Customer")]

    elasticsearch: Annotated[Service, Field(description="Elasticsearch instance")]

    postgres: Annotated[Service, Field(description="PostgreSQL instance")]

class AirflowHealthError(BaseModel):

    """

    Error during health check

    """

    message: Annotated[str | None, Field(description="Error message")] = None

    status_code: Annotated[

        int | None, Field(description="HTTP response status code")

    ] = None

class AirflowHealthStatus(str, Enum):

    HEALTHY = "healthy"

    UNHEALTHY = "unhealthy"

    UNKNOWN = "unknown"

class AirflowDagProcessorHealth(BaseModel):

    """

    Dag processor health

    """

    latest_dag_processor_heartbeat: Annotated[

        datetime | None,

        Field(description="Dag processor latest heartbeat"),

    ] = None

    status: Annotated[

        AirflowHealthStatus | None, Field(description="Status of the dag-processor")

    ] = None

class AirflowMetadatabaseHealth(BaseModel):

    """

    Database health

    """

    status: Annotated[

        AirflowHealthStatus | None, Field(description="The status of the database")

    ] = None

class AirflowSchedulerHealth(BaseModel):

    """

    Secheduler health

    """

    latest_scheduler_heartbeat: Annotated[

        datetime | None, Field(description="Scheduler latest heartbeat")

    ] = None

    status: Annotated[

        AirflowHealthStatus | None, Field(description="Status of the scheduler")

    ] = None

class AirflowTriggererHealth(BaseModel):

    """

    Triggerer health

    """

    latest_triggerer_heartbeat: Annotated[

        datetime | None, Field(description="Triggerer latest heartbeat")

    ] = None

    status: Annotated[

        AirflowHealthStatus | None, Field(description="Triggerer status")

    ] = None
class AirflowHealth(BaseModel):

    """

    Instance health

    """

    dag_processor: Annotated[

        AirflowDagProcessorHealth, Field(description="Dag processor health")

    ]

    metadatabase: Annotated[

        AirflowMetadatabaseHealth, Field(description="Database health")

    ]

    scheduler: Annotated[AirflowSchedulerHealth, Field(description="Scheduler health")]

    triggerer: Annotated[AirflowTriggererHealth, Field(description="Triggerer health")]

class AirflowHealthResult(BaseModel):

    """

    Instance healthcheck result

    """

    failure: Annotated[

        AirflowHealthError | None,

        Field(description="The error if healthcheck failed"),

    ] = None

    instance: Annotated[Instance, Field(description="The instance")]

    success: Annotated[

        AirflowHealth | None,

        Field(description="The healthcheck result (None if the healthcheck failed)"),

    ] = None

    def is_healthy(self) -> bool:

        return (

                self.failure is None

                and self.success.dag_processor.status == AirflowHealthStatus.HEALTHY

                and self.success.metadatabase.status == AirflowHealthStatus.HEALTHY

                and self.success.scheduler.status == AirflowHealthStatus.HEALTHY

                and self.success.triggerer.status == AirflowHealthStatus.HEALTHY

        )

    def is_unknown(self) -> bool:

        return self.failure is not None and self.failure.status_code == 408

class InstancePath(BaseModel):

    apcode: str

    customer_apcode: str

    customer_name: str

    env: str

    path: str

    release_id: str

    zone: str

    @staticmethod

    def parse(path: str):

        path_match = PATH_REGEX.match(path)

        if path_match is None:

            raise InvalidInstancePathError(path)

        return InstancePath(

            apcode=path_match["apcode"],

            customer_apcode=path_match["cust_apcode"],

            customer_name=path_match["cust"],

            env=path_match["env"],

            path=path,

            release_id=path_match["release_id"],

            zone=path_match["zone"],

        )
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

class Inventory:

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

            await self.__create_gitlab_branch(branch)

            await self.__delete_gitlab_file(

                path=eph.source.path,

                ref=branch,

                msg=f"{eph.name}: delete",

                author_email=author_email,

                author_name=author_name,

            )

            mr_id = await self.__create_gitlab_merge_request(

                branch, f"{eph.name}: delete"

            )

            if not no_merge:

                await self.__merge_gitlab_merge_request(mr_id)

    async def get_all(

            self, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Instance]:

        insts = []

        for env in envs:

            env_insts = await self.__get_all_from_env(env)

            release_ids = [inst.release_id for inst in env_insts]

            legacy_insts = await self.__get_all_legacies_from_env(env, release_ids)

            insts.extend(env_insts)

            insts.extend(legacy_insts)

        return insts

    async def get_all_by_customer_apcode(

            self, cust_apcode: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Instance]:

        insts = []

        for env in envs:

            env_insts = await self.__get_all_by_customer_apcode_from_env(

                env, cust_apcode

            )

            release_ids = [inst.release_id for inst in env_insts]

            legacy_insts = await self.__get_all_legacies_by_customer_apcode_from_env(

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

            env_insts = await self.__get_all_by_customer_name_from_env(env, cust_name)

            release_ids = [inst.release_id for inst in env_insts]

            legacy_insts = await self.__get_all_legacies_by_customer_name_from_env(

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

            cluster_tree = await self.__get_gitlab_repository_tree(

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

            env_ephs = await self.__get_all_ephemerals_from_env(env)

            ephs.extend(env_ephs)

        return ephs

    async def get_all_ephemerals_by_customer_apcode(

            self, cust_apcode: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Ephemeral]:

        ephs = []

        for env in envs:

            env_ephs = await self.__get_all_ephemerals_by_customer_apcode_from_env(

                env, cust_apcode

            )

            ephs.extend(env_ephs)

        return ephs

    async def get_all_ephemerals_by_customer_name(

            self, cust_name: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[Ephemeral]:

        ephs = []

        for env in envs:

            env_ephs = await self.__get_all_ephemerals_by_customer_name_from_env(

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

        return await self.__parse_all_ephemerals(path)

    async def get_all_health(

            self, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[AirflowHealth]:

        insts = await self.get_all(envs)

        return await self.__get_all_health(insts)

    async def get_all_health_by_customer_apcode(

            self, cust_apcode: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[AirflowHealth]:

        insts = await self.get_all_by_customer_apcode(cust_apcode, envs)

        return await self.__get_all_health(insts)

    async def get_all_health_by_customer_name(

            self, cust_name: str, envs: list[str] = list(LEGACY_REF_BY_ENV.keys())

    ) -> list[AirflowHealth]:

        insts = await self.get_all_by_customer_name(cust_name, envs)

        return await self.__get_all_health(insts)

    async def get_all_versions(self) -> list[str]:

        self.logger.debug("getting releases")

        releases = await self.__get_gitlab_releases()

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

        inst_path = await self.__get_instance_path_by_release_id(release_id)

        if inst_path is None:

            return await self.__get_legacy_by_release_id(release_id)

        else:

            return await self.__parse_instance(inst_path)

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

                cluster_file = await self.__get_gitlab_file(

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

            cust_tree = await self.__get_gitlab_repository_tree(

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

                        values_content_file = await self.__get_gitlab_file(

                            self.inv_project_id, values_path, self.inv_ref

                        )

                        self.logger.debug("parsing customer values")

                        values = yaml.load(

                            values_content_file.content, Loader=yaml.Loader

                        )

                        return self.__parse_customer_metadata(cust_name, apcode, values)

        return None

    async def get_ephemeral(self, release_id: str, number: int) -> Ephemeral | None:

        inst = await self.__get_ephemeral_from_zone("hprd", release_id, number)

        if inst is not None:

            return inst

        return await self.__get_ephemeral_from_zone("prod", release_id, number)

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

        meta_file = await self.__get_gitlab_file(

            self.inv_project_id, meta_path, self.inv_ref

        )

        values_file = await self.__get_gitlab_file(

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

        meta_tpl = self.__parse_template("metadata")

        meta_content = meta_tpl.render(inst=inst)

        version_match = VERSION_REGEX.match(cfg.version)

        if version_match is None:

            values_tpl = self.__parse_template("values-latest")

        else:

            version = semver.version.Version.parse(cfg.version)

            tpl_name = f"values-{version.major}.{version.minor}"

            try:

                values_tpl = self.__parse_template(tpl_name)

            except TemplateNotFound as err:

                self.logger.warn(f"template {tpl_name} doesn't exist, using latest")

                values_tpl = self.__parse_template("values-latest")

        secrets = [sync.secret for sync in inst.bucket_sync]

        secrets.extend([sync.secret for sync in inst.git_sync])

        if inst.smtp is not None:

            secrets.append(inst.smtp.secret)

        values_content = values_tpl.render(

            inst=inst,

            secret_stores=self.__secret_stores_from_secrets(secrets),

        )

        inst.source.metadata = yaml.load(meta_content, Loader=yaml.Loader)

        inst.source.values = yaml.load(values_content, Loader=yaml.Loader)

        meta_content = yaml.dump(inst.source.metadata)

        values_content = yaml.dump(inst.source.values)

        if not dry_run:

            await self.__create_gitlab_branch(branch)

            changed = False

            if meta_file is None:

                mr_name = f"{inst.name}: create"

                await self.__create_gitlab_file(

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

                updated = await self.__update_gitlab_file(

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

                await self.__create_gitlab_file(

                    path=values_path,

                    content=values_content,

                    ref=branch,

                    msg=f"{inst.name}: add values",

                    author_name=author_name,

                    author_email=author_email,

                )

                changed = True

            else:

                updated = await self.__update_gitlab_file(

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

                mr_id = await self.__create_gitlab_merge_request(branch, mr_name)

                if not no_merge:

                    await self.__merge_gitlab_merge_request(mr_id)

            else:

                await self.__delete_gitlab_branch(branch)

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

        values_tpl = self.__parse_template("eph")

        values_content = values_tpl.render(

            eph=eph,

            secret_stores=self.__secret_stores_from_secrets(secrets),

        )

        eph.values = yaml.load(values_content, Loader=yaml.Loader)

        values_content = yaml.dump(eph.values)

        if not dry_run:

            await self.__create_gitlab_branch(branch)

            values_file = await self.__get_gitlab_file(

                self.inv_project_id, values_path, branch

            )

            changed = False

            if values_file is None:

                mr_name = f"{eph.name}: create"

                await self.__create_gitlab_file(

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

                updated = await self.__update_gitlab_file(

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

                mr_id = await self.__create_gitlab_merge_request(branch, mr_name)

                if not no_merge:

                    await self.__merge_gitlab_merge_request(mr_id)

            else:

                await self.__delete_gitlab_branch(branch)

        return eph

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __create_gitlab_branch(self, branch: str):

        self.logger.debug(f"creating branch {branch}")

        async with ClientSession() as session:

            url = (

                f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/branches"

            )

            self.logger.debug(f"doing POST on {url}")

            async with session.post(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

                    json={

                        "branch": branch,

                        "ref": self.inv_ref,

                    },

            ) as resp:

                if resp.status != 201:

                    raise GitLabError(resp.status)

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __create_gitlab_file(

            self,

            path: str,

            content: str,

            ref: str,

            msg: str,

            author_name: str,

            author_email: str,

    ):

        self.logger.debug(f"creating {path}")

        url_path = urllib.parse.quote(path, safe="")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/files/{url_path}"

            self.logger.debug(f"doing POST on {url}")

            async with session.post(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

                    json={

                        "branch": ref,

                        "content": content,

                        "author_email": author_email,

                        "author_name": author_name,

                        "commit_message": msg,

                    },

            ) as resp:

                if resp.status != 201:

                    raise GitLabError(resp.status)

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __create_gitlab_merge_request(self, branch: str, title: str) -> int:

        self.logger.debug(f"creating merge request from {branch} into {self.inv_ref}")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/merge_requests"

            self.logger.debug(f"doing POST on {url}")

            async with session.post(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

                    json={

                        "source_branch": branch,

                        "target_branch": self.inv_ref,

                        "title": title,

                    },

            ) as resp:

                if resp.status != 201:

                    raise GitLabError(resp.status)

                data = await resp.json()

                return data["iid"]

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __delete_gitlab_branch(self, branch: str):

        self.logger.debug(f"deleting branch {branch}")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/branches/{branch}"

            self.logger.debug(f"doing DELETE on {url}")

            async with session.delete(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

            ) as resp:

                if resp.status != 204:

                    raise GitLabError(resp.status)

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __delete_gitlab_file(

            self, path: str, ref: str, msg: str, author_name: str, author_email: str

    ):

        self.logger.debug(f"deleting {path}")

        url_path = urllib.parse.quote(path, safe="")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/files/{url_path}"

            self.logger.debug(f"doing DELETE on {url}")

            async with session.delete(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

                    json={

                        "branch": ref,

                        "author_email": author_email,

                        "author_name": author_name,

                        "commit_message": msg,

                    },

            ) as resp:

                if resp.status != 200:

                    raise GitLabError(resp.status)
    async def __get_all_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str

    ) -> list[Instance]:

        insts = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(f"getting all instances of customer {cust_apcode} in {env}")

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                cust_path = cust_item["path"]

                if cust_item["mode"] == "040000" and cust_path.endswith(

                        f"-{cust_apcode}"

                ):

                    cust_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, cust_path, self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            inst = await self.__parse_instance(inst_path)

                            insts.append(inst)

        return insts

    async def __get_all_by_customer_name_from_env(

            self, env: str, cust_name: str

    ) -> list[Instance]:

        insts = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(f"getting all instances of customer {cust_name} in {env}")

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                cust_path = cust_item["path"]

                prefix = f"{env_path}/{cust_name}-"

                if cust_item["mode"] == "040000" and cust_path.startswith(prefix):

                    cust_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, cust_path, self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            inst = await self.__parse_instance(inst_path)

                            insts.append(inst)

        return insts

    async def __get_all_from_env(self, env: str) -> list[Instance]:

        insts = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(f"getting all instances from {zone}")

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                if cust_item["mode"] == "040000":

                    cust_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, cust_item["path"], self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            insts.append(await self.__parse_instance(inst_path))

        return insts

    async def __get_all_ephemerals_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str

    ) -> list[Ephemeral]:

        ephs = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(

            f"getting all ephemeral instances of customer {cust_apcode} in {env}"

        )

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                cust_path = cust_item["path"]

                if cust_item["mode"] == "040000" and cust_path.endswith(

                        f"-{cust_apcode}"

                ):

                    cust_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, cust_path, self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            env_ephs = await self.__parse_all_ephemerals(inst_path)

                            ephs.extend(env_ephs)

        return ephs

    async def __get_all_ephemerals_by_customer_name_from_env(

            self, env: str, cust_name: str

    ) -> list[Ephemeral]:

        ephs = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(

            f"getting all ephemeral instances of customer {cust_name} in {env}"

        )

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                cust_path = cust_item["path"]

                prefix = f"{env_path}/{cust_name}-"

                if cust_item["mode"] == "040000" and cust_path.startswith(prefix):

                    cust_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, cust_path, self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            env_ephs = await self.__parse_all_ephemerals(inst_path)

                            ephs.extend(env_ephs)

        return ephs

    async def __get_all_ephemerals_from_env(self, env: str) -> list[Ephemeral]:

        ephs = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(f"getting all ephemeral instances from {zone}")

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                if cust_item["mode"] == "040000":

                    cust_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, cust_item["path"], self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            env_ephs = await self.__parse_all_ephemerals(inst_path)

                            ephs.extend(env_ephs)

        return ephs
    async def __get_all_legacies_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str, release_ids: list[str]

    ) -> list[Instance]:

        insts = []

        self.logger.debug(

            f"getting all instances of customer {cust_apcode} in legacy inventory"

        )

        config_path = "config"

        config_tree = await self.__get_gitlab_repository_tree(

            self.legacy_inv_project_id, config_path, LEGACY_REF_BY_ENV[env]

        )

        for cust_item in config_tree:

            if cust_item["mode"] == "100644":

                insts.extend(

                    (

                        inst

                        for inst in await self.__parse_legacy_instances(

                        cust_item["path"], env

                    )

                        if inst.customer.apcode == cust_apcode

                           and inst.release_id not in release_ids

                    )

                )

        return insts

    async def __get_all_legacies_by_customer_name_from_env(

            self, env: str, cust_name: str, release_ids: list[str]

    ) -> list[Instance]:

        self.logger.debug(

            f"getting all instances of customer {cust_name} in legacy inventory"

        )

        inv_path = f"config/{cust_name}.yml"

        inv_file = await self.__get_gitlab_file(

            self.legacy_inv_project_id, inv_path, LEGACY_REF_BY_ENV[env]

        )

        if inv_file is not None:

            return [

                inst

                for inst in await self.__parse_legacy_instances(inv_path, env)

                if inst.release_id not in release_ids

            ]

        else:

            return []

    async def __get_all_legacies_from_env(

            self, env: str, release_ids: list[str]

    ) -> list[Instance]:

        insts = []

        self.logger.debug(f"getting all instances in legacy inventory")

        config_path = "config"

        config_tree = await self.__get_gitlab_repository_tree(

            self.legacy_inv_project_id, config_path, LEGACY_REF_BY_ENV[env]

        )

        for cust_item in config_tree:

            if cust_item["mode"] == "100644":

                insts.extend(

                    (

                        inst

                        for inst in await self.__parse_legacy_instances(

                        cust_item["path"], env

                    )

                        if inst.release_id not in release_ids

                    )

                )

        return insts

    async def __get_all_health(self, insts: list[Instance]):

        results = []

        for inst in insts:

            res = await self.__get_health(inst)

            results.append(res)

        return results

    async def __get_ephemeral_from_zone(

            self,

            zone: str,

            release_id: str,

            number: int,

    ) -> Ephemeral | None:

        self.logger.debug(f"searching ephemeral {release_id}-eph{number} from {zone}")

        zone_tree = await self.__get_gitlab_repository_tree(

            self.inv_project_id, zone, self.inv_ref

        )

        for env_item in zone_tree:

            if env_item["mode"] == "040000":

                env_tree = await self.__get_gitlab_repository_tree(

                    self.inv_project_id, env_item["path"], self.inv_ref

                )

                for cust_item in env_tree:

                    if cust_item["mode"] == "040000":

                        cust_tree = await self.__get_gitlab_repository_tree(

                            self.inv_project_id, cust_item["path"], self.inv_ref

                        )

                        for inst_item in cust_tree:

                            if inst_item["mode"] == "040000":

                                inst_path = InstancePath.parse(inst_item["path"])

                                if inst_path.release_id == release_id:

                                    eph_path = f"{inst_path}/eph{number}.yaml"

                                    eph_file = await self.__get_gitlab_file(

                                        self.inv_project_id, eph_path, self.inv_ref

                                    )

                                    if eph_file is not None:

                                        return await self.__parse_ephemeral(

                                            inst_path, number, eph_file

                                        )

        return None

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __get_gitlab_file(

            self, project_id: int, path: str, ref: str

    ) -> GitLabFile | None:

        self.logger.debug(f"getting {path}")

        url_path = urllib.parse.quote(path, safe="")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{project_id}/repository/files/{url_path}"

            self.logger.debug(f"doing get on {url}")

            async with session.get(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                    },

                    params={

                        "ref": ref,

                    },

            ) as resp:

                if resp.status == 200:

                    data = await resp.json()

                    content = base64.b64decode(data["content"])

                    return GitLabFile(

                        content=content,

                        path=path,

                    )

                elif resp.status == 404:

                    return None

                else:

                    raise GitLabError(resp.status)

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __get_gitlab_releases(self) -> list[str]:

        self.logger.debug(f"getting releases")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.chart_project_id}/releases"

            self.logger.debug(f"doing get on {url}")

            async with session.get(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                    },

            ) as resp:

                if resp.status == 200:

                    data = await resp.json()

                    return list([release["name"] for release in data])

                else:

                    raise GitLabError(resp.status)

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __get_gitlab_repository_tree(

            self, project_id: int, path: str, ref: str

    ) -> list[dict]:

        self.logger.debug(f"getting tree from {path} from {ref}")

        tree = []

        page = 1

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{project_id}/repository/tree"

            while True:

                self.logger.debug(f"doing get on {url}")

                async with session.get(

                        url,

                        headers={

                            "Authorization": f"Bearer {self.gitlab_token}",

                        },

                        params={

                            "page": page,

                            "path": path,

                            "per_page": 100,

                            "ref": ref,

                        },

                ) as resp:

                    if resp.status == 200:

                        tree.extend(await resp.json())

                        if resp.headers["X-Next-Page"] == "":

                            break

                        else:

                            page += 1

                    else:

                        raise GitLabError(resp.status)

        return tree

    async def __get_legacy_by_release_id(self, release_id: str) -> Instance | None:

        self.logger.debug(f"searching instance {release_id} in legacy inventory")

        for env in LEGACY_REF_BY_ENV:

            config_path = "config"

            config_tree = await self.__get_gitlab_repository_tree(

                self.legacy_inv_project_id, config_path, LEGACY_REF_BY_ENV[env]

            )

            for cust_item in config_tree:

                if cust_item["mode"] == "100644":

                    insts = await self.__parse_legacy_instances(cust_item["path"], env)

                    inst = next(

                        (inst for inst in insts if inst.release_id == release_id), None

                    )

                    if inst is not None:

                        return inst

        return None

    async def __get_health(self, inst: Instance) -> AirflowHealthResult:

        self.logger.debug(f"Airflow instance version: {inst.version}")

        def get_major_version(version_str: str) -> int:

            """

            Safely extracts the major version number from a version string.

            Returns 0 if the version string is invalid (e.g., "main", "dev", etc.).

            Args:

                version_str (str): Version string (e.g., "3.4.1", "4.0.1", or "main").

            Returns:

                int: The major version number if valid, otherwise 0.

            """

            try:

                # Check if the version string starts with a digit

                if not version_str[0].isdigit():

                    self.logger.warning(f"Invalid version format: '{version_str}'. Expected 'X.Y.Z'.")

                    return 0

                # Extract and return the major version

                major_version = int(version_str.split('.')[0])

                return major_version

            except (ValueError, IndexError, AttributeError) as e:

                self.logger.warning(f"Failed to parse version '{version_str}': {str(e)}")

                return 0

        # Get the major version (defaults to 0 if invalid)

        major_version = get_major_version(inst.version)

        self.logger.debug(f"Extracted major version: {major_version}")

        # Determine the health endpoint based on the major version

        url = f"{inst.url}/health"

        if major_version >= 4:

            self.logger.debug("Using Airflow 3.x+ health endpoint")

            url = f"{inst.url}/api/v2/monitor/health"

        self.logger.debug(f"sending get on {url}")

        try:

            timeout = ClientTimeout(total=10, connect=5)

            async with ClientSession(timeout=timeout) as session:

                async with session.get(url) as resp:

                    if resp.status == 200:

                        response_json = await resp.json()

                        data = AirflowHealth.model_validate(response_json)

                        return AirflowHealthResult(

                            instance=inst,

                            success=data,

                        )

                    else:

                        self.logger.error("url=%s, status=%s" % (url, resp.status))

                        return AirflowHealthResult(

                            failure=AirflowHealthError(

                                status_code=resp.status,

                            ),

                            instance=inst,

                        )

        except TimeoutError as err:

            self.logger.error("url=%s, exception=timeout" % url)

            return AirflowHealthResult(

                failure=AirflowHealthError(

                    message="Timeout",

                    status_code=408,

                ),

                instance=inst,

            )

        except Exception as err:

            self.logger.error("url=%s, exception=%s" % (url, err))

            return AirflowHealthResult(

                failure=AirflowHealthError(

                    message=str(err),

                ),

                instance=inst,

            )
    async def __get_instance_path_by_release_id(

            self, release_id: str

    ) -> InstancePath | None:

        inst_path = self.cache.get_instance_path(release_id)

        if inst_path is not None:

            self.logger.debug(f"instance {release_id} found in cache")

            return inst_path

        for zone in ["hprd", "prod"]:

            self.logger.debug(f"searching instance {release_id} from {zone}")

            zone_tree = await self.__get_gitlab_repository_tree(

                self.inv_project_id, zone, self.inv_ref

            )

            for env_item in zone_tree:

                if env_item["mode"] == "040000":

                    env_tree = await self.__get_gitlab_repository_tree(

                        self.inv_project_id, env_item["path"], self.inv_ref

                    )

                    for cust_item in env_tree:

                        if cust_item["mode"] == "040000":

                            cust_tree = await self.__get_gitlab_repository_tree(

                                self.inv_project_id, cust_item["path"], self.inv_ref

                            )

                            for inst_item in cust_tree:

                                if inst_item["mode"] == "040000":

                                    inst_path = InstancePath.parse(inst_item["path"])

                                    self.cache.save_instance_path(inst_path)

                                    if inst_path.release_id == release_id:

                                        return inst_path

        return None

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

    async def __merge_gitlab_merge_request(self, mr_id: int):

        self.logger.debug(f"rebasing merge request")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/merge_requests/{mr_id}/rebase"

            self.logger.debug(f"doing PUT on {url}")

            async with session.put(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

            ) as resp:

                if resp.status != 202:

                    raise GitLabError(resp.status)

            self.logger.debug(f"merging merge request")

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/merge_requests/{mr_id}/merge"

            self.logger.debug(f"doing PUT on {url}")

            async with session.put(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

            ) as resp:

                if resp.status != 200:

                    raise GitLabError(resp.status)

    async def __parse_all_ephemerals(self, path: InstancePath) -> list[Ephemeral]:

        ephs = []

        tree = await self.__get_gitlab_repository_tree(

            self.inv_project_id, path.path, self.inv_ref

        )

        for item in tree:

            name_match = EPH_FILENAME_REGEX.match(item["name"])

            if name_match is not None:

                number = name_match["number"]

                eph_file = await self.__get_gitlab_file(

                    self.inv_project_id, item["path"], self.inv_ref

                )

                eph = await self.__parse_ephemeral(path, number, eph_file)

                ephs.append(eph)

        return ephs

    def __parse_bucket_sync(self, inst_values: dict) -> list[BucketSync]:

        mapping = BucketSyncSecretMapping()

        global_bucket_sync = inst_values.get("bucketSync")

        if global_bucket_sync is not None:

            global_ext_secret = global_bucket_sync.get("externalSecret")

            if global_ext_secret is not None:

                global_props = global_ext_secret.get("defaultProperty")

                mapping = self.__parse_bucket_sync_mapping(global_props)

        bucket_sync = []

        secret_stores = inst_values.get("extraSecretStores", [])

        for bucket_dict in inst_values.get("buckets", []):

            ext_secret = bucket_dict["externalSecret"]

            secret = self.__parse_secret(secret_stores, ext_secret)

            bucket_props = ext_secret.get("property")

            bucket_sync.append(

                BucketSync(

                    bucket=bucket_dict["name"],

                    dags_path=bucket_dict.get("dagsPath", "/"),

                    mapping=self.__parse_bucket_sync_mapping(bucket_props, mapping),

                    secret=secret,

                    target_dir=bucket_dict.get("targetDir"),

                )

            )

        return bucket_sync
    def __parse_bucket_sync_mapping(

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

    wp_tree = await self.__get_gitlab_repository_tree(

        self.cluster_project_id, cluster_path, self.cluster_ref

    )

    for wp_item in wp_tree:

        if wp_item["name"] == "workerpoolclaims.yaml":

            wp_path = wp_item["path"]

            self.logger.debug(f"reading {wp_path}")

            wp_content_file = await self.__get_gitlab_file(

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

    inst = await self.__parse_instance(path)

    values = yaml.load(eph_file.content, Loader=yaml.Loader)

    return Ephemeral(

        bucket_sync=self.__parse_bucket_sync(values),

        git_sync=self.__parse_git_sync(values),

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
    def __parse_git_sync(self, inst_values: dict) -> list[GitSync]:

        mapping = GitSyncSecretMapping()

        global_git_sync = inst_values.get("gitSync")

        if global_git_sync is not None:

            global_ext_secret = global_git_sync.get("externalSecret")

            if global_ext_secret is not None:

                global_props = global_ext_secret.get("defaultProperty")

                mapping = self.__parse_git_sync_mapping(global_props)

        git_sync = []

        secret_stores = inst_values.get("extraSecretStores", [])

        for repo_dict in inst_values.get("gitRepositories", []):

            ext_secret = repo_dict["externalSecret"]

            props = ext_secret.get("property")

            git_sync.append(

                GitSync(

                    branch=repo_dict.get("branch"),

                    mapping=self.__parse_git_sync_mapping(props, mapping),

                    secret=self.__parse_secret(secret_stores, ext_secret),

                    target_dir=repo_dict.get("targetDir"),

                )

            )

        return git_sync
    def __parse_git_sync_mapping(

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
    async def __parse_instance(self, path: InstancePath) -> Instance:

        real_env = path.env

        if real_env == "dhdev":

            real_env = "dev"

        inst_name = instance_name(path.apcode, real_env, path.release_id)

        meta_path = f"{path.path}/metadata.yaml"

        self.logger.debug(f"reading {meta_path} from inventory ({self.inv_ref})")

        meta_file = await self.__get_gitlab_file(

            self.inv_project_id, meta_path, self.inv_ref

        )

        cust_values_path = f"{path.zone}/{path.env}/{path.customer_name}-{path.customer_apcode}/values.yaml"

        self.logger.debug(f"reading {cust_values_path} from inventory ({self.inv_ref})")

        cust_values_file = await self.__get_gitlab_file(

            self.inv_project_id, cust_values_path, self.inv_ref

        )

        inst_values_path = f"{path.path}/values.yaml"

        self.logger.debug(f"reading {inst_values_path} from inventory ({self.inv_ref})")

        inst_values_file = await self.__get_gitlab_file(

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

            bucket_sync=self.__parse_bucket_sync(inst_values),

            cluster=cluster,

            customer=cust_meta.customer,

            database_secret_creation=database_secret_creation,

            description=meta.get("description"),

            elasticsearch=cust_meta.elasticsearch,

            env=real_env,

            git_sync=self.__parse_git_sync(inst_values),

            myaccess_creation=myaccess_creation,

            name=inst_name,

            postgres=cust_meta.postgres,

            release_id=path.release_id,

            smtp=self.__parse_smtp(inst_values),

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
    async def __parse_legacy_instances(self, path: str, env: str) -> list[Instance]:

        insts = []

        path_match = LEGACY_PATH_REGEX.match(path)

        if path_match is None:

            raise InvalidInventoryPathError(path)

        self.logger.debug(

            f"reading {path} from legacy inventory ({LEGACY_REF_BY_ENV[env]})"

        )

        inv_file = await self.__get_gitlab_file(

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

                    bucket_sync=self.__parse_bucket_sync(inst_values),

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

                    git_sync=self.__parse_git_sync(inst_values),

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

                    smtp=self.__parse_smtp(inst_values),

                    url=url_from_name(inst_name),

                    version=inv_dict["trigger"]["branch"],

                    zone=zone,

                )

            )

        return insts
    def __parse_secret(self, secret_stores: list[dict], ext_secret: dict) -> Secret:

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

    def __parse_smtp(self, inst_values: dict) -> Smtp | None:

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

        secret = self.__parse_secret(secret_stores, ext_secret)

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

    def __parse_template(self, name: str) -> Template:

        self.logger.debug(f"parsing template {name}")

        return JINJA_ENV.get_template(f"{name}.j2")

    def __secret_stores_from_secrets(

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

    async def __update_gitlab_file(

            self,

            file: GitLabFile,

            content: str,

            ref: str,

            msg: str,

            author_name: str,

            author_email: str,

    ) -> bool:

        if file.content == content:

            return False

        url_path = urllib.parse.quote(file.path, safe="")

        async with ClientSession() as session:

            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/files/{url_path}"

            self.logger.debug(f"doing PUT on {url}")

            async with session.put(

                    url,

                    headers={

                        "Authorization": f"Bearer {self.gitlab_token}",

                        "Content-Type": "application/json",

                    },

                    json={

                        "branch": ref,

                        "content": content,

                        "author_email": author_email,

                        "author_name": author_name,

                        "commit_message": msg,

                    },

            ) as resp:

                if resp.status != 200:

                    raise GitLabError(resp.status)

        return True
