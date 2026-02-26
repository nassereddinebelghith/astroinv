from .._imports import *
from ..constants import *
from ..errors import *
from ..models import *
from ..cache import *
from ..utils import *
from ..parsers import *


class ParsingMixin:
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

async def _get_all_health(self, insts: list[Instance]):

        results = []

        for inst in insts:

            res = await self._get_health(inst)

            results.append(res)

        return results

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

                                        return await self._parse_ephemeral(

                                            inst_path, number, eph_file

                                        )

        return None

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

                eph = await self._parse_ephemeral(path, number, eph_file)

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
