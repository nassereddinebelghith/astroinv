from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


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
    """Best-effort parser for inventory directory paths.

    Expected shape:
      {zone}/{env}/{cust_name}-{cust_apcode}/{apcode}-{release_id}
    """

    raw: str
    zone: str
    env: str
    cust_name: str
    cust_apcode: str
    apcode: str
    release_id: str

    @staticmethod
    def parse(path: str) -> "InstancePath":
        parts = path.strip("/").split("/")
        if len(parts) < 4:
            raise ValueError(f"Invalid instance path: {path}")

        zone = parts[0]
        env = parts[1]

        cust_seg = parts[2]
        if "-" not in cust_seg:
            raise ValueError(f"Invalid customer segment: {cust_seg}")
        cust_name, cust_apcode = cust_seg.rsplit("-", 1)

        inst_seg = parts[3]
        if "-" not in inst_seg:
            raise ValueError(f"Invalid instance segment: {inst_seg}")
        apcode, release_id = inst_seg.split("-", 1)

        return InstancePath(
            raw=path,
            zone=zone,
            env=env,
            cust_name=cust_name,
            cust_apcode=cust_apcode,
            apcode=apcode,
            release_id=release_id,
        )

    @property
    def base_dir(self) -> str:
        return f"{self.zone}/{self.env}/{self.cust_name}-{self.cust_apcode}/{self.apcode}-{self.release_id}"


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


@dataclass(frozen=True)
class EphemeralConfig:
    source: str  # release_id
    number: int
    bucket_sync: Optional[dict] = None
    git_sync: Optional[dict] = None


@dataclass(frozen=True)
class InventorySnapshot:
    """Marketplace-friendly snapshot."""
    instances: list[Instance]
    generated_at_utc: str
