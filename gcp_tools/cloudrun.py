import os

from gcp_tools.utils import try_import

try_import("google.cloud.run_v2", "CloudRun")
from google.cloud import run_v2

from gcp_tools.pydocker import Docker
from gcp_tools.utils import get_auth_default


class CloudRun:

    _client = {}
    _jobs_client = {}

    def __init__(self, name=None, project=None, location="europe-west2"):
        self.project = project or os.getenv("PROJECT") or get_auth_default()[1]
        self.location = location
        self.parent = f"projects/{self.project}/locations/{self.location}"
        self.name = name
        self.full_name = f"{self.parent}/services/{self.name}"

        if self.project in CloudRun._client:
            self.client = CloudRun._client[self.project]
            self.jobs_client = CloudRun._jobs_client[self.project]
        else:
            self.client = run_v2.ServicesClient()
            self.jobs_client = run_v2.JobsClient()
            CloudRun._client[self.project] = self.client
            CloudRun._jobs_client[self.project] = self.jobs_client

    def ls_jobs(self, active_only=False, full_id=False):
        """
        List all jobs in the project.
        """
        jobs = self.jobs_client.list_jobs(parent=self.parent)
        if active_only:
            jobs = [f for f in jobs if f.terminal_condition.type_ == "Ready"]
        output = [x.name for x in jobs]
        if not full_id:
            output = [f.split("/")[-1] for f in output]
        return output

    def ls_services(self, active_only=False, full_id=False):
        """
        List all services in the project.
        """
        services = self.client.list_services(parent=self.parent)
        if active_only:
            services = [
                f for f in services if all([x.percent > 0 for x in f.traffic_statuses])
            ]
        output = [x.name for x in services]
        if not full_id:
            output = [f.split("/")[-1] for f in output]
        return output

    def ls(self, active_only=False, full_id=False, jobs=False):
        """
        List all services or jobs in the project.

        Args:
        - active_only (bool): Only list active services or jobs. Default is False.
        - full_id (bool): Return full resource IDs, e.g. 'projects/my-project/locations/europe-west2/services/my-service'. Default is False.
        - jobs (bool): List jobs instead of services. Default is False.

        Returns:
        - (list): A list of service or job names.
        """
        if jobs:
            return self.ls_jobs(active_only=active_only, full_id=full_id)
        return self.ls_services(active_only=active_only, full_id=full_id)

    def get(self, job=False):
        """
        Get a service or job by name.

        Args:
        - job (bool): Get a job instead of a service. Default is False.

        Returns:
        - (Service) or (Job): The service or job object.
        """
        if job:
            return self.jobs_client.get(name=self.full_name)
        return self.client.get(name=self.full_name)

    def build_and_push_docker_image(
        self, path=".", image_tag="latest", dockerfile="Dockerfile"
    ):
        """
        Build and push a Docker image to GCR.

        Args:
        - path (str): The path to the build context.
        - image_tag (str): The tag to apply to the image.
        - dockerfile (str): The path to the Dockerfile from the context.

        Returns:
        - (str): The GCR location of the image (e.g. 'gcr.io/my-project/my-image:latest')
        """
        docker = Docker(image_name=self.name, project=self.project, image_tag=image_tag)
        gcr_location = docker.build_and_push(path=path, dockerfile=dockerfile)
        if gcr_location is None:
            raise ValueError("Failed to build and push Docker image.")
        return gcr_location

    def deploy_service(self):
        pass

    def deploy_job(self):
        pass


if __name__ == "__main__":
    print(CloudRun().ls())
    print(CloudRun().ls(jobs=True))
