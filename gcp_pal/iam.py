import os

from gcp_pal.utils import try_import

try_import("google.cloud.iam_v2", "IAM")
from google.cloud import iam_v2


from gcp_pal.utils import get_auth_default


class IAM:

    _client = {}

    def __init__(self, name=None, project=None):

        if isinstance(name, str) and name.startswith("projects/"):
            name = name.split("/")[-1]

        self.name = name
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.parent = f"projects/{self.project}"

        if self.project in IAM._client:
            self.client = IAM._client[self.project]
        else:
            self.client = iam_v2.IAMClient()
            IAM._client[self.project] = self.client

    def __repr__(self):
        return f"IAM({self.name})"
    
    def ls(self):
        """
        List all roles in the project.

        Returns:
        - List[google.cloud.iam_v1.Role]: List of roles.
        """
        roles = self.client.list_roles(parent=self.parent)
        return roles
    