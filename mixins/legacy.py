from .._imports import *
from ..constants import *
from ..errors import *
from ..models import *
from ..cache import *
from ..utils import *
from ..parsers import *


class LegacyMixin:
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
