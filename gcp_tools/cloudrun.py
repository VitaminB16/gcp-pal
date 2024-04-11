import os

from gcp_tools.utils import try_import

try_import("google.cloud.run_v2", "CloudRun")
from google.cloud import run_v2
from google.cloud.run_v2 import types

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

        if self.project in CloudRun._client:
            self.client = CloudRun._client[self.project]
            self.jobs_client = CloudRun._jobs_client[self.project]
        else:
            self.client = run_v2.CloudRunServiceClient()
            self.jobs_client = run_v2.CloudRunJobsClient()
            CloudRun._client[self.project] = self.client
            CloudRun._jobs_client[self.project] = self.jobs_client

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

    def deploy_service():
        pass

    def deploy_job():
        pass


if __name__ == "__main__":
    cr = CloudRun(name="my-service", location="europe-west2")
