class GitLabError(RuntimeError):
    """Raised on GitLab API errors."""

    def __init__(self, status: int, message: str | None = None) -> None:
        super().__init__(message or f"GitLab API error: HTTP {status}")
        self.status = status


class InstanceNotFoundError(RuntimeError):
    pass


class InvalidInventoryPathError(RuntimeError):
    pass


class InvalidClusterNameError(RuntimeError):
    pass


class SecretStoreNotFoundError(RuntimeError):
    pass
