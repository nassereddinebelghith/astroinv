from __future__ import annotations


def zone_from_env(env: str) -> str:
    """Best-effort mapping for the inventory layout.

    Adjust this function if your inventory structure differs.
    """
    if env in ("prd", "prod", "hprd"):
        return "prod"
    if env in ("pprd",):
        return "hprd"
    return "nonprod"


def instance_name(apcode: str, env: str, release_id: str) -> str:
    return f"{apcode}-{env}-{release_id}"


def ephemeral_name(inst_name: str, number: int) -> str:
    return f"{inst_name}-eph{number}"


def is_database_secret_creation_enabled(inst_values: dict) -> bool:
    """Normalize booleans coming from inconsistent YAML structures."""
    v = inst_values.get("databaseSecretCreation")
    if isinstance(v, bool):
        return v
    return bool(inst_values.get("database", {}).get("secretCreationEnabled", False))


def url_from_name(inst_values: dict, inst_name: str) -> str:
    """Compute the public URL for an Airflow instance.

    Prefer explicit values in YAML; otherwise fall back to a conventional URL.
    """
    base = inst_values.get("url") or inst_values.get("airflow", {}).get("url")
    if base:
        return str(base)
    return f"https://{inst_name}"
