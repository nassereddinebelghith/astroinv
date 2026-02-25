class InventoryError(Exception):
    pass


class GitLabError(InventoryError):
    def __init__(self, status: int):
        super().__init__(f"GitLab returned an error {status}")


class InvalidClusterNameError(InventoryError):
    def __init__(self, cluster_name: str):
        super().__init__(f"{cluster_name} is not a valid cluster name")


class InvalidInstancePathError(InventoryError):
    def __init__(self, path: str):
        super().__init__(f"{path} is not a valid instance path")


class InvalidInventoryPathError(InventoryError):
    def __init__(self, path: str):
        super().__init__(f"{path} is not a valid inventory path")


class FileNotFoundError(InventoryError):
    def __init__(self, path: str):
        super().__init__(f"file not found: {path}")


class SecretStoreNotFoundError(InventoryError):
    def __init__(self, secret_store_name: str):
        super().__init__(f"secret store {secret_store_name} not found")


class CustomerNotFoundError(InventoryError):
    def __init__(self, customer: str):
        super().__init__(f"customer {customer} not found")


class InstanceNotFoundError(InventoryError):
    def __init__(self, release_id: str):
        super().__init__(f"instance {release_id} not found")
