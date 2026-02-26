from ._imports import *

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

class AirflowHealthError(BaseModel):
    """
    Error during health check
    """
    message: Annotated[str | None, Field(description="Error message")] = None
    status_code: Annotated[
        int | None, Field(description="HTTP response status code")
    ] = None

