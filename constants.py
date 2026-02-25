import re
import semver.version

TEMPLATES_PATH = "templates"

# Ex: custname-ap12345
PATH_REGEX = re.compile(
    r"^(?P<zone>hprd|prod)/(?P<env>dev|int|uat|pprd|prd|dhdev)/(?P<cust_name>[^-]+)-(?P<cust_apcode>ap[0-9]{5})/(?P<apcode>ap[0-9]{5})-(?P<release_id>[a-z0-9]{10})$"
)

# Ex: config/customer.yml
LEGACY_PATH_REGEX = re.compile(r"^config/(?P<cust>[a-z0-9]+)\.ya?ml$")

# Ex: eph12.yaml or eph12.yml
EPH_FILENAME_REGEX = re.compile(r"^eph(?P<number>\d+)\.ya?ml$")

# cluster name format
CLUSTER_REGEX = re.compile(
    r"^iks-(?P<apcode>[a-z0-9]+)-(?P<zone>[a-z0-9]+)-[a-z0-9]+$"
)

# apcode format
APCODE_REGEX = re.compile(r"^a(p|[0-9])[0-9]{5}$")

# version format
VERSION_REGEX = re.compile(r"^(?P<major>[0-9]+)\.(?P<minor>[0-9]+)\.(?P<patch>[0-9]+)$")

LEGACY_REF_BY_ENV = {
    "dev": "dev",
    "int": "int",
    "uat": "uat",
    "pprd": "pprd",
    "prd": "prd",
    "hprd": "hprd",
    "prod": "prod",
}

FIRST_VERSION = semver.version.Version(major=0, minor=1, patch=0)

GITLAB_TRY_WAIT = 1
GITLAB_MAX_TRIES = 3
