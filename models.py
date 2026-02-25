from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any

from .exceptions import InvalidInstancePathError
from .constants import PATH_REGEX


@dataclass(frozen=True)
class Service:
    host: str
    port: int


@dataclass(frozen=True)
class Cluster:
    account: str
    name: str


@dataclass(frozen=True)
class Customer:
    apcode: str
    name: str
    snow_label: str


@dataclass(frozen=True)
class Secret:
    engine: str
    namespace: str
    path: str
    url: str


@dataclass(frozen=True)
class SecretStore:
    namespace: str
    path: str
    server: str


@dataclass(frozen=True)
class Source:
    path: str
    project_id: int
    ref: str
    values: dict


@dataclass(frozen=True)
class InstancePath:
    zone: str
    env: str
    cust_name: str
    cust_apcode: str
    apcode: str
    release_id: str

    @staticmethod
    def from_str(path: str) -> "InstancePath":
        match = PATH_REGEX.match(path)
        if match is None:
            raise InvalidInstancePathError(path)

        return InstancePath(
            zone=match["zone"],
            env=match["env"],
            cust_name=match["cust_name"],
            cust_apcode=match["cust_apcode"],
            apcode=match["apcode"],
            release_id=match["release_id"],
        )


@dataclass(frozen=True)
class Instance:
    apcode: str
    cluster: Cluster
    customer: Customer
    database_secret_creation: bool
    description: Optional[str]
    elasticsearch: Service
    env: str
    myaccess_creation: bool
    name: str
    postgres: Service
    release_id: str
    source: Source
    smtp: Optional[dict]
    url: str
    version: str
    zone: str
    bucket_sync: Optional[dict] = None
    git_sync: Optional[dict] = None


@dataclass(frozen=True)
class Ephemeral:
    instance: Instance
    name: str
    number: int
    source: Source
    bucket_sync: Optional[dict] = None
    git_sync: Optional[dict] = None
    secret_stores: Optional[dict[str, SecretStore]] = None


@dataclass(frozen=True)
class AirflowHealthError:
    message: str
    status_code: int


@dataclass(frozen=True)
class AirflowHealthResult:
    instance: Instance
    success: Optional[Any] = None
    failure: Optional[AirflowHealthError] = None


class Action(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
