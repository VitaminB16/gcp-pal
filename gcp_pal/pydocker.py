import os

from gcp_pal.utils import try_import

from gcp_pal.utils import get_auth_default, log, ModuleHandler


class Docker:

    _client = None

    def __init__(
        self,
        name,
        project=None,
        tag="latest",
        destination=None,
        repository="docker",
        location="europe-west2",
    ):
        """
        Initialize a Docker object.

        Args:
        - name (str): The name of the Docker image.
        - project (str): The GCP project ID. Defaults to the PROJECT environment variable or the default auth project.
        - tag (str): The tag of the Docker image. Defaults to 'latest'.
        - destination (str): The destination of the pushed image. Defaults to gcr.io/{project}/{name}:{tag}.
        - repository (str): The name of the Artifact Registry repository. Defaults to 'docker'.
        """
        self.docker = ModuleHandler("docker").please_import(who_is_calling="Docker")
        if self._client is None:
            self.client = self.docker.from_env()
            Docker._client = self.client
        else:
            self.client = self._client

        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.name = name
        self.tag = tag
        self.repository = repository
        self.location = location
        gcr_dest = f"gcr.io/{self.project}/{self.name}:{self.tag}"
        ar_dest = f"{self.location}-docker.pkg.dev/{self.project}/{self.repository}/{self.name}:{self.tag}"
        self.default_dest = gcr_dest
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
        log(f"Docker - Building image '{self.name}:{self.tag}' from {dockerfile}...")
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
                log(f"Docker - Image '{self.name}:{self.tag}' built successfully.")
                return
        log(f"Docker - Image '{self.name}:{self.tag}' failed to build.")
        return

    def push(self, verbose=False, destination=None, **kwargs):
        """
        Push the built image to GCR.

        Args:
        - verbose (bool): Whether to stream the output of the push command.
        - destination (str): The destination of the pushed image. Defaults to gcr.io/{project}/{name}:{tag}.
        - kwargs: Additional arguments to pass to the Docker images push command.

        Returns:
        - (str) The destination of the pushed image (or None if an error occurred)
        """
        destination = destination or self.destination
        log(f"Docker - Pushing image to {destination}...")
        stream = True if verbose else False
        output = self.client.images.push(
            destination,
            stream=stream,
            decode=True,
            **kwargs,
        )
        # Check for success
        for line in output:
            if "error" in line:
                log(f"Docker - Error: {line}")
                return
        log(f"Docker - Pushed '{self.name}' -> {destination}.")
        return destination

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
        - (str) The destination of the pushed image (or None if an error occurred)
        """
        self.build(path=path, dockerfile=dockerfile, verbose=verbose, **kwargs)
        output = self.push(verbose=verbose, **kwargs)
        return output


# Example usage
if __name__ == "__main__":
    context = "samples/cloud_run"
    Docker("test-app").build(context)
    Docker("test-app").push()
