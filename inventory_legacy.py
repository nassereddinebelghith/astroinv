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


class LegacyMixin:
    async def _get_legacy_by_release_id(self, release_id: str) -> Instance | None:

        self.logger.debug(f"searching instance {release_id} in legacy inventory")

        for env in LEGACY_REF_BY_ENV:

            config_path = "config"

            config_tree = await self._get_gitlab_repository_tree(

                self.legacy_inv_project_id, config_path, LEGACY_REF_BY_ENV[env]

            )

            for cust_item in config_tree:

                if cust_item["mode"] == "100644":

                    insts = await self._parse_legacy_instances(cust_item["path"], env)

                    inst = next(

                        (inst for inst in insts if inst.release_id == release_id), None

                    )

                    if inst is not None:

                        return inst

        return None

