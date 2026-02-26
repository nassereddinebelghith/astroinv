from .._imports import *
from ..constants import *
from ..errors import *
from ..models import *
from ..cache import *
from ..utils import *
from ..parsers import *


class GitLabMixin:
    async def _create_gitlab_branch(self, branch: str):
        self.logger.debug(f"creating branch {branch}")
        async with ClientSession() as session:
            url = (
                f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/branches"
            )
            self.logger.debug(f"doing POST on {url}")
            async with session.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "branch": branch,
                        "ref": self.inv_ref,
                    },
            ) as resp:
                if resp.status != 201:
                    raise GitLabError(resp.status)

    async def _create_gitlab_file(
            self,
            path: str,
            content: str,
            ref: str,
            msg: str,
            author_name: str,
            author_email: str,
    ):

        self.logger.debug(f"creating {path}")
        url_path = urllib.parse.quote(path, safe="")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/files/{url_path}"
            self.logger.debug(f"doing POST on {url}")
            async with session.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "branch": ref,
                        "content": content,
                        "author_email": author_email,
                        "author_name": author_name,
                        "commit_message": msg,
                    },
            ) as resp:
                if resp.status != 201:
                    raise GitLabError(resp.status)

    async def _create_gitlab_merge_request(self, branch: str, title: str) -> int:
        self.logger.debug(f"creating merge request from {branch} into {self.inv_ref}")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/merge_requests"
            self.logger.debug(f"doing POST on {url}")
            async with session.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "source_branch": branch,
                        "target_branch": self.inv_ref,
                        "title": title,
                    },
            ) as resp:
                if resp.status != 201:
                    raise GitLabError(resp.status)
                data = await resp.json()
                return data["iid"]

    async def _delete_gitlab_branch(self, branch: str):
        self.logger.debug(f"deleting branch {branch}")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/branches/{branch}"
            self.logger.debug(f"doing DELETE on {url}")
            async with session.delete(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
            ) as resp:
                if resp.status != 204:
                    raise GitLabError(resp.status)

    async def _delete_gitlab_file(
            self, path: str, ref: str, msg: str, author_name: str, author_email: str
    ):

        self.logger.debug(f"deleting {path}")
        url_path = urllib.parse.quote(path, safe="")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/repository/files/{url_path}"
            self.logger.debug(f"doing DELETE on {url}")
            async with session.delete(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "branch": ref,
                        "author_email": author_email,
                        "author_name": author_name,
                        "commit_message": msg,
                    },
            ) as resp:
                if resp.status != 200:
                    raise GitLabError(resp.status)

    async def _get_gitlab_file(
            self, project_id: int, path: str, ref: str
    ) -> GitLabFile | None:
        self.logger.debug(f"getting {path}")
        url_path = urllib.parse.quote(path, safe="")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{project_id}/repository/files/{url_path}"
            self.logger.debug(f"doing get on {url}")
            async with session.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                    },
                    params={
                        "ref": ref,
                    },
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = base64.b64decode(data["content"])
                    return GitLabFile(
                        content=content,
                        path=path,
                    )
                elif resp.status == 404:
                    return None
                else:
                    raise GitLabError(resp.status)

    async def _get_gitlab_releases(self) -> list[str]:
        self.logger.debug(f"getting releases")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{self.chart_project_id}/releases"
            self.logger.debug(f"doing get on {url}")
            async with session.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                    },
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return list([release["name"] for release in data])
                else:
                    raise GitLabError(resp.status)

    async def _get_gitlab_repository_tree(
            self, project_id: int, path: str, ref: str
    ) -> list[dict]:
        self.logger.debug(f"getting tree from {path} from {ref}")
        tree = []
        page = 1

        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{project_id}/repository/tree"
            while True:
                self.logger.debug(f"doing get on {url}")
                async with session.get(
                        url,
                        headers={
                            "Authorization": f"Bearer {self.gitlab_token}",
                        },
                        params={
                            "page": page,
                            "path": path,
                            "per_page": 100,
                            "ref": ref,
                        },
                ) as resp:
                    if resp.status == 200:
                        tree.extend(await resp.json())
                        if resp.headers["X-Next-Page"] == "":
                            break
                        else:
                            page += 1
                    else:
                        raise GitLabError(resp.status)
        return tree

    async def _merge_gitlab_merge_request(self, mr_id: int):
        self.logger.debug(f"rebasing merge request")
        async with ClientSession() as session:
            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/merge_requests/{mr_id}/rebase"
            self.logger.debug(f"doing PUT on {url}")
            async with session.put(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
            ) as resp:
                if resp.status != 202:
                    raise GitLabError(resp.status)
            self.logger.debug(f"merging merge request")
            url = f"{self.gitlab_url}/projects/{self.inv_project_id}/merge_requests/{mr_id}/merge"
            self.logger.debug(f"doing PUT on {url}")
            async with session.put(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.gitlab_token}",
                        "Content-Type": "application/json",
                    },
            ) as resp:
                if resp.status != 200:
                    raise GitLabError(resp.status)