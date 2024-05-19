import os

from gcp_pal.utils import get_auth_default, log, ClientHandler, ModuleHandler


class Project:

    def __init__(self, project_id: str = None, folder: str = None):
        self.project_id = (
            project_id
            or os.environ.get("PROJECT")
            or get_auth_default(errors="ignore")[1]
        )
        self.folder = folder
        self.parent = f"folders/{self.folder}" if self.folder else None
        self.name = f"projects/{self.project_id}"

        self.resourcemanager = ModuleHandler("google.cloud").please_import(
            "resourcemanager_v3", who_is_calling="Project"
        )
        self.ProjectsClient = self.resourcemanager.ProjectsClient
        self.types = self.resourcemanager.types
        self.client = ClientHandler(self.ProjectsClient).get()

    def __repr__(self):
        return f"Project({self.project_id})"

    def create(self):
        """
        Creates a project.

        Returns:
        - google.cloud.resourcemanager_v3.types.Project
        """
        project = self.types.Project(project_id=self.project_id, parent=self.parent)
        output = self.client.create_project(project=project)
        output = output.result(timeout=300)
        log(f"Project - Created project '{self.project_id}'.")
        return output

    def delete(self):
        """
        Deletes a project. It will be marked for deletion, and will be deleted after 30 days.

        Returns:
        - None
        """
        self.client.delete_project(name=self.name)
        log(f"Project - Deleted project '{self.project_id}'.")

    def undelete(self):
        """
        Restores a project which has been marked for deletion.

        Returns:
        - None
        """
        self.client.undelete_project(name=self.name)
        log(f"Project - Restored project '{self.project_id}'.")

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

    def get(self):
        """
        Retrieves the project identified by the specified project ID.

        Returns:
        - google.cloud.resourcemanager_v3.types.Project
        """
        project = self.client.get_project(name=self.name)
        return project

    def number(self):
        """
        Retrieves the project number.

        Returns:
        - (str) Project number (e.g. '123456789012')
        """
        got = self.get()
        output = got.name.split("/")[-1]
        return output


if __name__ == "__main__":
    # print(Project().ls(active_only=True))
    print(Project("vitaminb16").get())
