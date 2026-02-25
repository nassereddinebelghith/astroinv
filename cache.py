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

