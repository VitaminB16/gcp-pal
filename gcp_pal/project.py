import os
from gcp_pal.utils import try_import

try_import("google.cloud.resourcemanager_v3", "Project")

from google.cloud.resourcemanager_v3 import ProjectsClient
from google.cloud.resourcemanager_v3.types import Project as ProjectType
from gcp_pal.utils import get_auth_default


class Project:

    _clients = {}

    def __init__(self, project: str = None, folder: str = None):
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.folder = folder
        self.parent = f"folders/{self.folder}" if self.folder else None

        if self.project in Project._clients:
            self.client = Project._clients[self.project]
        else:
            self.client = ProjectsClient()
            Project._clients[self.project] = self.client

    def __repr__(self):
        return f"Project({self.project})"
