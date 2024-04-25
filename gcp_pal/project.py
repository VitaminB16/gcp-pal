import os
from gcp_pal.utils import try_import

try_import("google.cloud.resourcemanager_v3", "Project")

from google.cloud.resourcemanager_v3 import ProjectsClient
from google.cloud.resourcemanager_v3.types import Project as ProjectType
from gcp_pal.utils import get_auth_default, log


class Project:

    _clients = {}

    def __init__(self, project_id: str = None, folder: str = None):
        self.project_id = (
            project_id or os.environ.get("PROJECT") or get_auth_default()[1]
        )
        self.folder = folder
        self.parent = f"folders/{self.folder}" if self.folder else None
        self.name = f"projects/{self.project_id}"

        if self.project_id in Project._clients:
            self.client = Project._clients[self.project_id]
        else:
            self.client = ProjectsClient()
            Project._clients[self.project_id] = self.client

    def __repr__(self):
        return f"Project({self.project_id})"

    def create(self):
        """
        Creates a project.

        Returns:
        - google.cloud.resourcemanager_v3.types.Project
        """
        project = ProjectType(project_id=self.project_id, parent=self.parent)
        output = self.client.create_project(project=project)
        output = output.result(timeout=300)
        log(f"Project - Created project '{self.project_id}'.")
        return output

    def delete(self):
        """
        Deletes a project.

        Returns:
        - None
        """
        self.client.delete_project(name=self.name)
        log(f"Project - Deleted project '{self.project_id}'.")

    def ls(self, active_only: bool = True):
        """
        Lists all projects which are available to the caller.

        Args:
        - active_only: (bool) If True, only return active projects. This filters out projects that are marked for deletion.

        Returns:
        - List of project IDs
        """
        projects = self.client.search_projects()
        projects = [x for x in projects]
        if active_only:
            projects = [x for x in projects if x.state.name == "ACTIVE"]
        project_ids = [x.project_id for x in projects]
        return project_ids


if __name__ == "__main__":
    print(Project().ls(active_only=True))
