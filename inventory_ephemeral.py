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


class EphemeralMixin:
    async def _get_all_ephemerals_by_customer_apcode_from_env(

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

            env_tree = await self._get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                cust_path = cust_item["path"]

                if cust_item["mode"] == "040000" and cust_path.endswith(

                        f"-{cust_apcode}"

                ):

                    cust_tree = await self._get_gitlab_repository_tree(

                        self.inv_project_id, cust_path, self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            env_ephs = await self._parse_all_ephemerals(inst_path)

                            ephs.extend(env_ephs)

        return ephs


    async def _get_all_ephemerals_by_customer_name_from_env(

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

            env_tree = await self._get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                cust_path = cust_item["path"]

                prefix = f"{env_path}/{cust_name}-"

                if cust_item["mode"] == "040000" and cust_path.startswith(prefix):

                    cust_tree = await self._get_gitlab_repository_tree(

                        self.inv_project_id, cust_path, self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            env_ephs = await self._parse_all_ephemerals(inst_path)

                            ephs.extend(env_ephs)

        return ephs


    async def _get_all_ephemerals_from_env(self, env: str) -> list[Ephemeral]:

        ephs = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(f"getting all ephemeral instances from {zone}")

        for env in envs:

            env_path = f"{zone}/{env}"

            env_tree = await self._get_gitlab_repository_tree(

                self.inv_project_id, env_path, self.inv_ref

            )

            for cust_item in env_tree:

                if cust_item["mode"] == "040000":

                    cust_tree = await self._get_gitlab_repository_tree(

                        self.inv_project_id, cust_item["path"], self.inv_ref

                    )

                    for inst_item in cust_tree:

                        if inst_item["mode"] == "040000":

                            inst_path = InstancePath.parse(inst_item["path"])

                            env_ephs = await self._parse_all_ephemerals(inst_path)

                            ephs.extend(env_ephs)

        return ephs

    async def _get_ephemeral_from_zone(

            self,

            zone: str,

            release_id: str,

            number: int,

    ) -> Ephemeral | None:

        self.logger.debug(f"searching ephemeral {release_id}-eph{number} from {zone}")

        zone_tree = await self._get_gitlab_repository_tree(

            self.inv_project_id, zone, self.inv_ref

        )

        for env_item in zone_tree:

            if env_item["mode"] == "040000":

                env_tree = await self._get_gitlab_repository_tree(

                    self.inv_project_id, env_item["path"], self.inv_ref

                )

                for cust_item in env_tree:

                    if cust_item["mode"] == "040000":

                        cust_tree = await self._get_gitlab_repository_tree(

                            self.inv_project_id, cust_item["path"], self.inv_ref

                        )

                        for inst_item in cust_tree:

                            if inst_item["mode"] == "040000":

                                inst_path = InstancePath.parse(inst_item["path"])

                                if inst_path.release_id == release_id:

                                    eph_path = f"{inst_path}/eph{number}.yaml"

                                    eph_file = await self._get_gitlab_file(

                                        self.inv_project_id, eph_path, self.inv_ref

                                    )

                                    if eph_file is not None:

                                        return await self.__parse_ephemeral(

                                            inst_path, number, eph_file

                                        )

        return None

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

