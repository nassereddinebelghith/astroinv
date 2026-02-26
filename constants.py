from ._imports import *

APCODE_REGEX = re.compile(r"^a(p|[0-9])[0-9]{5}$")
CLUSTER_REGEX = re.compile(r"^iks-(?P<apcode>[a-z0-9]+)-(?P<zone>[a-z0-9]+)-[a-z0-9]+$")
EPH_FILENAME_REGEX = re.compile(r"eph(?P<number>[0-9]+)\.yaml$")
LEGACY_REF_BY_ENV = {

    "dev": "dev",

    "int": "int",

    "qual": "qualif",

    "pprd": "pprd",

    "prod": "prod",

}
PATH_REGEX = re.compile(

    r"^(?P<zone>prod|hprd)/(?P<env>[a-z0-9]+)/(?P<cust>[a-z0-9]+)-(?P<cust_apcode>[a-z0-9]+)/(?P<apcode>[a-z0-9]+)-(?P<release_id>[a-z0-9]+)$"

)
LEGACY_PATH_REGEX = re.compile(r"^config/(?P<cust>[a-z0-9]+)\.ya?ml$")
VERSION_REGEX = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
TEMPLATES_PATH = Path(__file__).parent.joinpath(f"templates")
JINJA_ENV = Environment(

    loader=FileSystemLoader(TEMPLATES_PATH),

    keep_trailing_newline=True,

)
GITLAB_TRY_WAIT = 5
GITLAB_MAX_TRIES = 10
FIRST_VERSION = semver.version.Version(major=3, minor=4, patch=0)
