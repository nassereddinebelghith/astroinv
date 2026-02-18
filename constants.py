from __future__ import annotations

import re

# ----------------------------
# GitLab Retry / Concurrency
# ----------------------------

# Retry policy for transient GitLab/API failures.
GITLAB_TRY_WAIT_SECONDS = 1
GITLAB_MAX_TRIES = 3

# Marketplace scanning is throughput-oriented: moderate concurrency is safe and fast.
# If your GitLab is fragile or heavily shared, reduce to 10.
DEFAULT_GITLAB_CONCURRENCY = 15

# Health checks hit Airflow endpoints, not GitLab. Keep separate tuning.
DEFAULT_HEALTH_CONCURRENCY = 20

# ----------------------------
# Cache TTLs (seconds)
# ----------------------------
# Marketplace usually tolerates slight staleness. These values reduce repeated calls during bursts.
TREE_CACHE_TTL_SECONDS = 120
FILE_CACHE_TTL_SECONDS = 120

# ----------------------------
# Inventory conventions
# ----------------------------

LEGACY_REF_BY_ENV: dict[str, str] = {
    "dev": "dev",
    "int": "int",
    "uat": "uat",
    "pprd": "pprd",
    "prd": "prd",
    "hprd": "hprd",
    "prod": "prod",
}

# Example: eph12.yaml or eph12.yml
EPH_FILENAME_REGEX = re.compile(r"^eph(?P<number>\d+)\.ya?ml$")

# Example legacy file: config/customer.yml
LEGACY_PATH_REGEX = re.compile(r"^config/(?P<cust>[^/]+)\.ya?ml$")

# Conservative cluster name validation.
CLUSTER_REGEX = re.compile(r"^[a-zA-Z0-9\-]+$")
