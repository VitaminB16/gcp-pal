import os
import docker

from gcp_tools.utils import get_auth_default, log


class Docker:

    _client = None

    def __init__(self, name, project=None, tag="latest", destination=None):
        if self._client is None:
            self.client = docker.from_env()
            Docker._client = self.client
        else:
            self.client = self._client

        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.name = name
        self.tag = tag
        self.default_dest = f"gcr.io/{self.project}/{self.name}:{self.tag}"
        self.destination = destination or self.default_dest

    def build(self, path=".", dockerfile="Dockerfile", verbose=False, **kwargs):
        """
        Build a Docker image from a Dockerfile.

        Args:
        - path (str): The path to the build context.
        - dockerfile (str): The relative path to the Dockerfile from the context path.
        - verbose (bool): Whether to stream the output of the build command.
        - kwargs: Additional arguments to pass to the Docker images build command.

        Returns:
        - None
        """
        log(f"Docker - Building image {self.destination} from {dockerfile}...")
        _, output = self.client.images.build(
            path=path,
            tag=self.destination,
            dockerfile=dockerfile,
            **kwargs,
        )
        if verbose:
            for line in output:
                log(line)
        # Check for success
        for line in output:
            stream = line.get("stream")
            if stream is not None and "Successfully built" in stream:
                log(f"Docker - Image '{self.name}' built successfully.")
                return
        log(f"Docker - Image '{self.name}' failed to build.")

    def push(self, verbose=False, **kwargs):
        """
        Push the built image to GCR.

        Args:
        - verbose (bool): Whether to stream the output of the push command.
        - kwargs: Additional arguments to pass to the Docker images push command.

        Returns:
        - None
        """
        log(f"Docker - Pushing image to {self.destination}...")
        stream = True if verbose else False
        output = self.client.images.push(
            self.destination,
            stream=stream,
            decode=True,
            **kwargs,
        )
        # Check for success
        for line in output:
            if "error" in line:
                log(f"Docker - Error: {line}")
                return
        log(f"Docker - Pushed '{self.name}' -> {self.destination}.")

    def build_and_push(
        self, path=".", dockerfile="Dockerfile", verbose=False, **kwargs
    ):
        """
        Build and push a Docker image to GCR.

        Args:
        - path (str): The path to the build context.
        - dockerfile (str): The relative path to the Dockerfile from the context path.
        - verbose (bool): Whether to stream the output of the build and push commands.
        - kwargs: Additional arguments to pass to the Docker images build and push commands.

        Returns:
        - None
        """
        self.build(path=path, dockerfile=dockerfile, verbose=verbose, **kwargs)
        self.push(verbose=verbose, **kwargs)


# Example usage
if __name__ == "__main__":
    context = "samples/cloud_run"
    Docker("test-app").build(context)
    Docker("test-app").push()
