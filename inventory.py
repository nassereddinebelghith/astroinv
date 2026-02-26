from ._imports import *
from .constants import *
from .errors import *
from .models import *
from .cache import *
from .utils import *
from .parsers import *
from .mixins.gitlab import GitLabMixin
from .mixins.parsing import ParsingMixin
from .mixins.legacy import LegacyMixin


class Inventory(GitLabMixin, ParsingMixin, LegacyMixin):

    def __init__(self, *args, **kwargs):
        # Backward-compatible: original code uses init() not __init__
        self.init(*args, **kwargs)

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

async def __create_gitlab_branch(self, branch: str):

        return await self._create_gitlab_branch(branch)


async def __create_gitlab_file(

            self,

            path: str,

            content: str,

            ref: str,

            msg: str,

            author_name: str,

            author_email: str,

    ):

        return await self._create_gitlab_file(path, content, ref, msg, author_name, author_email)


async def __create_gitlab_merge_request(self, branch: str, title: str) -> int:

        return await self._create_gitlab_merge_request(branch, title)


async def __delete_gitlab_branch(self, branch: str):

        return await self._delete_gitlab_branch(branch)


async def __delete_gitlab_file(

            self, path: str, ref: str, msg: str, author_name: str, author_email: str

    ):

        return await self._delete_gitlab_file(path, ref, msg, author_name, author_email)


async def __get_all_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str

    ) -> list[Instance]:

        return await self._get_all_by_customer_apcode_from_env(env, cust_apcode)


async def __get_all_by_customer_name_from_env(

            self, env: str, cust_name: str

    ) -> list[Instance]:

        return await self._get_all_by_customer_name_from_env(env, cust_name)


async def __get_all_from_env(self, env: str) -> list[Instance]:

        return await self._get_all_from_env(env)


async def __get_all_ephemerals_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str

    ) -> list[Ephemeral]:

        return await self._get_all_ephemerals_by_customer_apcode_from_env(env, cust_apcode)


async def __get_all_ephemerals_by_customer_name_from_env(

            self, env: str, cust_name: str

    ) -> list[Ephemeral]:

        return await self._get_all_ephemerals_by_customer_name_from_env(env, cust_name)


async def __get_all_ephemerals_from_env(self, env: str) -> list[Ephemeral]:

        return await self._get_all_ephemerals_from_env(env)


async def __get_all_legacies_by_customer_apcode_from_env(

            self, env: str, cust_apcode: str, release_ids: list[str]

    ) -> list[Instance]:

        return await self._get_all_legacies_by_customer_apcode_from_env(env, cust_apcode, release_ids)


async def __get_all_legacies_by_customer_name_from_env(

            self, env: str, cust_name: str, release_ids: list[str]

    ) -> list[Instance]:

        return await self._get_all_legacies_by_customer_name_from_env(env, cust_name, release_ids)


async def __get_all_legacies_from_env(

            self, env: str, release_ids: list[str]

    ) -> list[Instance]:

        return await self._get_all_legacies_from_env(env, release_ids)


async def __get_all_health(self, insts: list[Instance]):

        return await self._get_all_health(insts)


async def __get_ephemeral_from_zone(

            self,

            zone: str,

            release_id: str,

            number: int,

    ) -> Ephemeral | None:

        return await self._get_ephemeral_from_zone(zone, release_id, number)


async def __get_gitlab_file(

            self, project_id: int, path: str, ref: str

    ) -> GitLabFile | None:

        return await self._get_gitlab_file(project_id, path, ref)


async def __get_gitlab_releases(self) -> list[str]:

        return await self._get_gitlab_releases()


async def __get_gitlab_repository_tree(

            self, project_id: int, path: str, ref: str

    ) -> list[dict]:

        return await self._get_gitlab_repository_tree(project_id, path, ref)


async def __get_legacy_by_release_id(self, release_id: str) -> Instance | None:

        return await self._get_legacy_by_release_id(release_id)


async def __get_health(self, inst: Instance) -> AirflowHealthResult:

        return await self._get_health(inst)


async def __get_instance_path_by_release_id(

            self, release_id: str

    ) -> InstancePath | None:

        return await self._get_instance_path_by_release_id(release_id)


async def __merge_gitlab_merge_request(self, mr_id: int):

        return await self._merge_gitlab_merge_request(mr_id)


async def __parse_all_ephemerals(self, path: InstancePath) -> list[Ephemeral]:

        return await self._parse_all_ephemerals(path)


def __parse_bucket_sync(self, inst_values: dict) -> list[BucketSync]:

        return self._parse_bucket_sync(inst_values)


def __parse_bucket_sync_mapping(

            self,

            props: dict | None,

            default: BucketSyncSecretMapping = BucketSyncSecretMapping(),

    ) -> BucketSyncSecretMapping:

        return self._parse_bucket_sync_mapping(props, default)

