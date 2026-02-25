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


class HealthMixin:
    async def _get_all_health(self, insts: list[Instance]):

        results = []

        for inst in insts:

            res = await self._get_health(inst)

            results.append(res)

        return results


    async def _get_health(self, inst: Instance) -> AirflowHealthResult:

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
