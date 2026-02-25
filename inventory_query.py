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


class QueryMixin:
    async def _get_all_by_customer_apcode_from_env(

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

                            inst = await self._parse_instance(inst_path)

                            insts.append(inst)

        return insts


    async def _get_all_by_customer_name_from_env(

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

                            inst = await self._parse_instance(inst_path)

                            insts.append(inst)

        return insts


    async def _get_all_from_env(self, env: str) -> list[Instance]:

        insts = []

        zone = zone_from_env(env)

        envs = [env]

        if env == "dev":

            envs.append("dhdev")

        self.logger.debug(f"getting all instances from {zone}")

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

                            insts.append(await self._parse_instance(inst_path))

        return insts


    async def _get_all_legacies_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str, release_ids: list[str]

    ) -> list[Instance]:

        insts = []

        self.logger.debug(

            f"getting all instances of customer {cust_apcode} in legacy inventory"

        )

        config_path = "config"

        config_tree = await self._get_gitlab_repository_tree(

            self.legacy_inv_project_id, config_path, LEGACY_REF_BY_ENV[env]

        )

        for cust_item in config_tree:

            if cust_item["mode"] == "100644":

                insts.extend(

                    (

                        inst

                        for inst in await self._parse_legacy_instances(

                        cust_item["path"], env

                    )

                        if inst.customer.apcode == cust_apcode

                           and inst.release_id not in release_ids

                    )

                )

        return insts


    async def _get_all_legacies_by_customer_name_from_env(

            self, env: str, cust_name: str, release_ids: list[str]

    ) -> list[Instance]:

        self.logger.debug(

            f"getting all instances of customer {cust_name} in legacy inventory"

        )

        inv_path = f"config/{cust_name}.yml"

        inv_file = await self._get_gitlab_file(

            self.legacy_inv_project_id, inv_path, LEGACY_REF_BY_ENV[env]

        )

        if inv_file is not None:

            return [

                inst

                for inst in await self._parse_legacy_instances(inv_path, env)

                if inst.release_id not in release_ids

            ]

        else:

            return []


    async def _get_all_legacies_from_env(

            self, env: str, release_ids: list[str]

    ) -> list[Instance]:

        insts = []

        self.logger.debug(f"getting all instances in legacy inventory")

        config_path = "config"

        config_tree = await self._get_gitlab_repository_tree(

            self.legacy_inv_project_id, config_path, LEGACY_REF_BY_ENV[env]

        )

        for cust_item in config_tree:

            if cust_item["mode"] == "100644":

                insts.extend(

                    (

                        inst

                        for inst in await self._parse_legacy_instances(

                        cust_item["path"], env

                    )

                        if inst.release_id not in release_ids

                    )

                )

        return insts


    async def _get_instance_path_by_release_id(

            self, release_id: str

    ) -> InstancePath | None:

        inst_path = self.cache.get_instance_path(release_id)

        if inst_path is not None:

            self.logger.debug(f"instance {release_id} found in cache")

            return inst_path

        for zone in ["hprd", "prod"]:

            self.logger.debug(f"searching instance {release_id} from {zone}")

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

                                    self.cache.save_instance_path(inst_path)

                                    if inst_path.release_id == release_id:

                                        return inst_path

        return None

    @retry(wait=wait_fixed(GITLAB_TRY_WAIT), stop=stop_after_attempt(GITLAB_MAX_TRIES))

