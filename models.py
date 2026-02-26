from ._imports import *
from .constants import *
from .errors import *

class GitLabFile(BaseModel):

    content: str

    path: str

class Secret(BaseModel):

    """

    A Vault secret

    """

    engine: Annotated[str, Field(description="Vault secret engine")]

    namespace: Annotated[str, Field(description="Vault namespace")]

    path: Annotated[str, Field(description="Path to secret in engine")]

    url: Annotated[str, Field(description="Vault base URL")]

    def hash(self):

        return hash((self.url, self.namespace, self.engine, self.path))

    def secret_store_name(self) -> str:

        ec = self.namespace.split("/")[-1]

        return f"{ec}-{self.engine}".lower()

class SecretStore(BaseModel):

    namespace: str

    path: str

    server: str

class Sync(BaseModel):

    secret: Annotated[Secret, Field(description="Secret used for synchronization")]

    target_dir: Annotated[

        str | None,

        Field(description="Path to DAGs directory"),

    ] = None

class GitSyncSecretMapping(BaseModel):

    """

    Secrets mapping for git synchronization

    """

    branch: Annotated[

        str,

        Field(description="Name of the property of the secret containing the branch"),

    ] = "branch"

    password: Annotated[

        str,

        Field(description="Name of the property of the secret containing the pasword"),

    ] = "password"

    url: Annotated[

        str,

        Field(

            description="Name of the property of the secret containing repository URL"

        ),

    ] = "repo"

    user: Annotated[

        str,

        Field(

            description="The name of the property of the secret that contains usrname"

        ),

    ] = "user"

class BucketSyncSecretMapping(BaseModel):

    """

    Secret mapping for bucket synchronization

    """

    access_key_id: Annotated[

        str,

        Field(

            description="The name of the property of the secret that contains access key ID"

        ),

    ] = "cos_hmac_keys_access_key_id"

    secret_access_key: Annotated[

        str,

        Field(

            description="Name of the property of the secret containing secret access key"

        ),

    ] = "cos_hmac_keys_secret_access_key"

class GitSync(Sync):

    """

    Git synchronization

    """

    branch: Annotated[

        str | None,

        Field(

            description="Branch used for synchronizing (overrides the branch in the Vault secret)"

        ),

    ] = None

    mapping: Annotated[

        GitSyncSecretMapping, Field(description="Secret property mapping")

    ] = GitSyncSecretMapping()

class BucketSync(Sync):

    """

    A bucket synchronization

    """

    bucket: Annotated[str, Field(description="Bucket name")]

    dags_path: Annotated[str, Field(description="Path to the dags in the bucket")]

    mapping: Annotated[

        BucketSyncSecretMapping, Field(description="Secret property mapping")

    ] = BucketSyncSecretMapping()

class Cluster(BaseModel):

    """

    A cluster

    """

    account: Annotated[str, Field(description="IBM account")]

    name: Annotated[str, Field(description="IKS cluster name")]

class Customer(BaseModel):

    """

    A customer

    """

    apcode: Annotated[str, Field(description="Customer APCODE")]

    name: str = Annotated[str, Field(description="Customer name")]

    snow_label: str = Annotated[

        str, Field(description="Label to reference customers in MyAccess rights")

    ]

class Service(BaseModel):

    """

    A service

    """

    host: Annotated[str, Field(description="The host of the service")]

    port: Annotated[int, Field(description="The port of the service")]

class Source(BaseModel):

    """

    Instance inventory source

    """

    metadata: Annotated[dict, Field(description="Raw instance metadata")] = {}

    path: Annotated[

        str, Field(description="Path to the directory/file describing the instance")

    ]

    project_id: Annotated[

        int,

        Field(description="ID of GitLab project containing the instance definition"),

    ]

    ref: Annotated[

        str,

        Field(

            description="GitLab reference of the project containing the instance definition"

        ),

    ]

    values: Annotated[dict, Field(description="Raw values for the instance")] = {}

class SmtpSecretMapping(BaseModel):

    """

    Secret mapping used for SMTP configuration

    """

    host: Annotated[

        str,

        Field(description="Name of the property of the host secret containing host"),

    ] = "host"

    password: Annotated[

        str,

        Field(description="Name of the property of the password secret"),

    ] = "password"

    port: Annotated[

        str,

        Field(description="Name of the property of the port secret"),

    ] = "port"

    sender: Annotated[

        str,

        Field(description="Name of the property of the sender secret"),

    ] = "sender"

    user: Annotated[

        str,

        Field(description="The name of the property of the secret that contains user"),

    ] = "user"

class Smtp(BaseModel):

    """

    SMTP configuration

    """

    mapping: Annotated[

        SmtpSecretMapping, Field(description="Secret property mapping")

    ] = SmtpSecretMapping()

    secret: Annotated[Secret, Field(description="Secret for SMTP configuration")]

class InstanceConfig(BaseModel):

    """

    Instance configuration

    """

    apcode: Annotated[

        str, Field(description="Instance APCODE"), AfterValidator(is_valid_apcode)

    ]

    bucket_sync: Annotated[

        list[BucketSync], Field(description="List of bucket synchronization")

    ] = []

    cluster_name: Annotated[

        str,

        Field(description="IKS cluster name"),

        AfterValidator(is_valid_cluster_name),

    ]

    customer_apcode: Annotated[

        str, Field(description="Customer APCODE"), AfterValidator(is_valid_apcode)

    ]

    database_secret_creation: Annotated[

        bool,

        Field(

            description="True if database secret must be generated at provisioning, false if it already exists"

        ),

    ] = True

    description: Annotated[

        str | None, Field(description="Description of the instance")

    ] = None

    env: Annotated[

        str,

        Field(description="Environment of the instance"),

        AfterValidator(is_valid_env),

    ]

    git_sync: Annotated[

        list[GitSync], Field(description="List of git synchronization")

    ] = []

    myaccess_creation: Annotated[

        bool,

        Field(

            description="True if NyAccess rights/teams must be created at provisioning, otherwise false"

        ),

    ] = True

    release_id: Annotated[

        str,

        Field(

            description="Instance release ID",

            default_factory=generate_release_id,

        ),

    ]

    smtp: Annotated[Smtp | None, Field(description="SMTP configuration")] = None

    version: Annotated[str, Field(description="OaaS version to use")]

class Instance(BaseModel):

    """

    Instance definition

    """

    apcode: Annotated[str, Field(description="Instance APCODE")]

    bucket_sync: Annotated[

        list[BucketSync], Field(description="List of bucket synchronization")

    ] = []

    cluster: Annotated[Cluster, Field(description="Cluster where the instance resides")]

    customer: Annotated[Customer, Field(description="Customer that owns the instance")]

    database_secret_creation: Annotated[

        bool,

        Field(

            description="True if database secret must be generated at provisioning, false if it already exists"

        ),

    ] = True

    description: Annotated[

        str | None, Field(description="Description of the instance")

    ] = None

    elasticsearch: Annotated[

        Service, Field(description="Elasticsearch used by the instance")

    ]

    env: Annotated[str, Field(description="Environment of the instance")]

    git_sync: Annotated[

        list[GitSync], Field(description="List of git synchronization")

    ] = []

    myaccess_creation: Annotated[

        bool,

        Field(

            description="True if NyAccess rights/teams must be created at provisioning, false otherwise"

        ),

    ] = True

    name: Annotated[str, Field(description="Instance name")]

    postgres: Annotated[Service, Field(description="PostgreSQL used by the instance")]

    release_id: Annotated[str, Field(description="Instance release ID")]

    source: Annotated[Source, Field(description="Source describing the instance")]

    smtp: Annotated[Smtp | None, Field(description="SMTP configuration")] = None

    url: Annotated[str, Field(description="Instance URL")]

    version: Annotated[str, Field(description="OaaS version to use")]

    zone: Annotated[str, Field(description="Environment zone")]

    def config(self) -> InstanceConfig:

        return InstanceConfig(

            apcode=self.apcode,

            bucket_sync=self.bucket_sync,

            cluster_name=self.cluster.name,

            customer_apcode=self.customer.apcode,

            database_secret_creation=is_database_secret_creation_enabled(

                self.source.values

            ),

            description=self.description,

            env=self.env,

            git_sync=self.git_sync,

            myaccess_creation=is_myaccess_creation_enabled(self.source.values),

            release_id=self.release_id,

            smtp=self.smtp,

            version=self.version,

        )

class EphemeralConfig(BaseModel):

    """

    Ephemeral instance configuration

    """

    bucket_sync: Annotated[

        list[BucketSync],

        Field(description="List of bucket synchronization, overrides source instance"),

    ] = []

    git_sync: Annotated[

        list[GitSync],

        Field(description="Git synchronization, overrides source instance"),

    ] = []

    number: Annotated[

        int,

        Field(description="Unique ID of the ephemeral instance"),

        is_strictly_positive,

    ]

    source: Annotated[str, Field(description="Source instance name")]

class Ephemeral(BaseModel):

    """

    Ephemeral instance definition

    """

    bucket_sync: Annotated[

        list[BucketSync],

        Field(description="List of bucket synchronization, overrides source instance"),

    ] = []

    git_sync: Annotated[

        list[GitSync],

        Field(description="List of git synchronization, overrides source instance"),

    ] = []

    instance: Annotated[Instance, Field(description="Source instance")]

    name: Annotated[str, Field(description="Name of ephemeral instance")]

    number: Annotated[int, Field(description="Unique ID of the ephemeral instance")]

    source: Annotated[

        Source, Field(description="Source that describes the ephemeral instance")

    ]

    values: Annotated[

        dict, Field(description="Custom values of the ephemeral instance")

    ] = {}

class CustomerMetadata(BaseModel):

    """

    Customer metadata

    """

    customer: Annotated[Customer, Field(description="Customer")]

    elasticsearch: Annotated[Service, Field(description="Elasticsearch instance")]

    postgres: Annotated[Service, Field(description="PostgreSQL instance")]

class AirflowHealthStatus(str, Enum):

    HEALTHY = "healthy"

    UNHEALTHY = "unhealthy"

    UNKNOWN = "unknown"

class AirflowDagProcessorHealth(BaseModel):

    """

    Dag processor health

    """

    latest_dag_processor_heartbeat: Annotated[

        datetime | None,

        Field(description="Dag processor latest heartbeat"),

    ] = None

    status: Annotated[

        AirflowHealthStatus | None, Field(description="Status of the dag-processor")

    ] = None

class AirflowMetadatabaseHealth(BaseModel):

    """

    Database health

    """

    status: Annotated[

        AirflowHealthStatus | None, Field(description="The status of the database")

    ] = None

class AirflowSchedulerHealth(BaseModel):

    """

    Secheduler health

    """

    latest_scheduler_heartbeat: Annotated[

        datetime | None, Field(description="Scheduler latest heartbeat")

    ] = None

    status: Annotated[

        AirflowHealthStatus | None, Field(description="Status of the scheduler")

    ] = None

class AirflowTriggererHealth(BaseModel):

    """

    Triggerer health

    """

    latest_triggerer_heartbeat: Annotated[

        datetime | None, Field(description="Triggerer latest heartbeat")

    ] = None

    status: Annotated[

        AirflowHealthStatus | None, Field(description="Triggerer status")

    ] = None

class AirflowHealth(BaseModel):

    """

    Instance health

    """

    dag_processor: Annotated[

        AirflowDagProcessorHealth, Field(description="Dag processor health")

    ]

    metadatabase: Annotated[

        AirflowMetadatabaseHealth, Field(description="Database health")

    ]

    scheduler: Annotated[AirflowSchedulerHealth, Field(description="Scheduler health")]

    triggerer: Annotated[AirflowTriggererHealth, Field(description="Triggerer health")]

class AirflowHealthResult(BaseModel):

    """

    Instance healthcheck result

    """

    failure: Annotated[

        AirflowHealthError | None,

        Field(description="The error if healthcheck failed"),

    ] = None

    instance: Annotated[Instance, Field(description="The instance")]

    success: Annotated[

        AirflowHealth | None,

        Field(description="The healthcheck result (None if the healthcheck failed)"),

    ] = None

    def is_healthy(self) -> bool:

        return (

                self.failure is None

                and self.success.dag_processor.status == AirflowHealthStatus.HEALTHY

                and self.success.metadatabase.status == AirflowHealthStatus.HEALTHY

                and self.success.scheduler.status == AirflowHealthStatus.HEALTHY

                and self.success.triggerer.status == AirflowHealthStatus.HEALTHY

        )

    def is_unknown(self) -> bool:

        return self.failure is not None and self.failure.status_code == 408

class InstancePath(BaseModel):

    apcode: str

    customer_apcode: str

    customer_name: str

    env: str

    path: str

    release_id: str

    zone: str

    @staticmethod

    def parse(path: str):

        path_match = PATH_REGEX.match(path)

        if path_match is None:

            raise InvalidInstancePathError(path)

        return InstancePath(

            apcode=path_match["apcode"],

            customer_apcode=path_match["cust_apcode"],

            customer_name=path_match["cust"],

            env=path_match["env"],

            path=path,

            release_id=path_match["release_id"],

            zone=path_match["zone"],

        )

