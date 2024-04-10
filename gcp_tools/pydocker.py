import os
import docker

from gcp_tools.utils import get_auth_default, log


class Docker:

    _client = None

    def __init__(self, image_name, project=None, image_tag="latest", destination=None):
        if self._client is None:
            self.client = docker.from_env()
            Docker._client = self.client
        else:
            self.client = self._client

        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.image_name = image_name
        self.image_tag = image_tag
        self.default_dest = f"gcr.io/{self.project}/{self.image_name}:{self.image_tag}"
        self.destination = destination or self.default_dest

    def build(self, context=".", dockerfile="Dockerfile", verbose=False, **kwargs):
        """
        Build a Docker image from a Dockerfile.

        Args:
        - context (str): The path to the build context.
        - dockerfile (str): The path to the Dockerfile.
        - verbose (bool): Whether to stream the output of the build command.
        - kwargs: Additional arguments to pass to the Docker images build command.

        Returns:
        - None
        """
        log(f"Building Docker image {self.destination} from Dockerfile {dockerfile}...")
        _, output = self.client.images.build(
            path=context,
            tag=self.destination,
            dockerfile=dockerfile,
            **kwargs,
        )
        if verbose:
            for line in output:
                log(line)

    def push(self, verbose=False, **kwargs):
        """
        Push the built image to GCR.

        Args:
        - verbose (bool): Whether to stream the output of the push command.
        - kwargs: Additional arguments to pass to the Docker images push command.

        Returns:
        - None
        """
        log(f"Pushing Docker image to {self.destination}...")
        stream = True if verbose else False
        self.client.images.push(
            self.destination,
            stream=stream,
            decode=True,
            **kwargs,
        )


# Example usage
if __name__ == "__main__":
    context = "samples/cloud_run"
    Docker("test-app").build(context)
    Docker("test-app").push()
