class InventoryError(Exception):

    pass

class GitLabError(InventoryError):

    def init(self, status: int):

        super().__init__(f"GitLab returned an error {status}")

class InvalidClusterNameError(InventoryError):

    def init(self, cluster_name: str):

        super().__init__(f"{cluster_name} is not a valid cluster name")

class InvalidInstancePathError(InventoryError):

    def init(self, path: str):

        super().__init__(f"{path} is not a valid instance path")

class InvalidInventoryPathError(InventoryError):

    def init(self, path: str):

        super().__init__(f"{path} is not a valid inventory path")

class FileNotFoundError(InventoryError):

    def init(self, path: str):

        super().__init__(f"{path} doesn't exist")

class SecretStoreNotFoundError(InventoryError):

    def init(self, secret_store: str):

        super().__init__(f"Secret store {secret_store} doesn't exist")

class CustomerNotFoundError(InventoryError):

    def init(self, apcode: str, env: str):

        super().__init__(f"Customer {apcode} does not exist in environment {env}")

class InstanceNotFoundError(InventoryError):

    def init(self, release_id: str):

        super().__init__(f"Instance {release_id} doesn't exist")

def ephemeral_name(inst_name: str, number: int) -> str:

    return f"{inst_name}-eph{number}"

def generate_release_id() -> str:

    release_uuid = str(uuid.uuid4())

    return release_uuid.split("-")[0]

def instance_name(apcode: str, env: str, release_id: str) -> str:

    return f"astronomer-{apcode}-{env}-{release_id}"

def is_database_secret_creation_enabled(inst_values: dict) -> bool:

    database_secret_creation = False

    database = inst_values.get("database")

    if database is not None:

        database_secret = database.get("secret")

        if database_secret is not None:

            database_secret_creation = database_secret.get("create", False)

    return database_secret_creation

def is_myaccess_creation_enabled(inst_values: dict) -> bool:

    myaccess_creation = True

    myaccess = inst_values.get("myaccess")

    if myaccess is not None:

        myaccess_creation = myaccess.get("create", True)

    return myaccess_creation

def is_strictly_positive(number: int) -> int:

    if number <= 0:

        raise ValueError(f"{number} should be > 0")

    return number

def is_valid_apcode(apcode: str) -> str:

    if APCODE_REGEX.match(apcode) is None:

        raise ValueError(f"{apcode} is not a valid AP Code")

    return apcode

def is_valid_cluster_name(name: str) -> str:

    if CLUSTER_REGEX.match(name) is None:

        raise ValueError(f"{name} is not a valid cluster name")

    return name

def is_valid_env(env: str) -> str:

    if env not in LEGACY_REF_BY_ENV:

        raise ValueError(f"{env} is not a valid environment")

    return env

def url_from_name(name: str) -> str:

    return f"https://{name}.data.cloud.net.intra"

def zone_from_env(env: str) -> str:

    if env == "prod" or env == "pprd":

        return "prod"

    else:

        return "hprd"

