import threading

from gcp_pal.utils import ModuleHandler, ClientHandler, log, get_default_arg


class ArtifactRegistry:

    _sentinel = object()

    def __init__(
        self,
        path: str = "",
        project: str = _sentinel,
        location: str = _sentinel,
        repository: str = None,
        image: str = None,
        version: str = None,
        tag: str = None,
    ):
        """
        Initialize an ArtifactRegistry object.

        Args:
        - path (str): The path to the resource. This follows the hierarchy `repository/image/version/tag`.
                      If a full path is given (starting with `'projects/'`), the `project`, `location`, `repository`,
                      `image`, and `tag` will be extracted from the path.
                      The path can be either one of the forms:
            - `projects/PROJECT/locations/LOCATION/repositories/REPOSITORY/packages/IMAGE/versions/sha256:VERSION`
            - `REPOSITORY/IMAGE/VERSION`
            - `REPOSITORY/IMAGE:TAG`
        - project (str): The GCP project ID. Defaults to the `PROJECT` environment variable or the default auth project.
        - location (str): The location of the Artifact Registry. Defaults to `'europe-west2'`.
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image (SHA256 hash).
        - tag (str): The tag of the image (e.g. `'latest'`). If a tag is provided, the `version` will be ignored.
        """
        if isinstance(path, str) and path.startswith("projects/"):
            path = path.split("/")
            path = "/".join(path[1::2])
            path = path.replace("%2F", "/")
            # Extract project and location from path and leave path to be repository/image/package
            try:
                project = path.split("/")[0]
            except IndexError:
                pass
            try:
                location = path.split("/")[1]
            except IndexError:
                # If path is provided as 'projects/project', assume location is intentionally left out
                location = None
            try:
                path = "/".join(path.split("/")[2:])
            except IndexError:
                pass

        self.project = (
            project if project is not self._sentinel else get_default_arg("project")
        )
        self.location = (
            location if location is not self._sentinel else get_default_arg("location")
        )
        self.repository = repository
        self.image = image
        self.tag = tag
        self.version = version
        try:
            self.repository = path.split("/")[0] if path != "" else self.repository
        except IndexError:
            pass
        try:
            self.image = path.split("/")[1:]
            if len(self.image) > 1:
                self.image = "/".join(self.image[:-1])
            elif self.image == []:
                self.image = None or image
            else:
                self.image = self.image[0]
        except IndexError:
            pass
        try:
            self.version = path.split("sha256:")[1]
        except IndexError:
            pass
        try:
            self.tag = self.image.split(":")[1]
            self.image = self.image.split(":")[0]
        except (IndexError, AttributeError):
            pass
        if self.tag:
            self.version = None
        self.level = self._get_level()
        self.path = self._get_path()
        self.artifactregistry_v1 = ModuleHandler(
            "google.cloud.artifactregistry_v1"
        ).please_import(who_is_calling="ArtifactRegistry")
        self.client = ClientHandler(
            self.artifactregistry_v1.ArtifactRegistryClient
        ).get()
        self.types = self.artifactregistry_v1.types
        self.FailedPrecondition = ModuleHandler(
            "google.api_core.exceptions"
        ).please_import("FailedPrecondition", who_is_calling="ArtifactRegistry")
        self.NotFound = ModuleHandler("google.api_core.exceptions").please_import(
            "NotFound", who_is_calling="ArtifactRegistry"
        )
        self.parent = self._get_parent()
        if self.project is None:
            raise ValueError("Project is required.")

    def __repr__(self):
        return f"ArtifactRegistry({self.project}/{self.location}/{self.path})"

    def _get_level(self, repository=None, image=None, version=None, tag=None):
        """
        Get the level of the path. Can be either 'project', 'location', 'repository', or 'file'.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.
        - tag (str): The tag of the image.

        Returns:
        - str: The level of the path.
        """
        if tag or self.tag:
            return "tag"
        elif version or self.version:
            return "version"
        elif image or self.image:
            return "image"
        elif repository or self.repository:
            return "repository"
        elif self.location:
            return "location"
        elif self.project:
            return "project"
        else:
            return None

    def _get_path(self, shorten=True):
        """
        Get the path to the resource.

        Args:
        - shorten (bool): Whether to shorten the path to `project/location/repository/file`.

        Returns:
        - str: The path to the resource.
        """
        path = ""
        if self.level is None:
            return path
        elif self.level == "project":
            path = f"projects/{self.project}"
        elif self.level == "location":
            path = f"projects/{self.project}/locations/{self.location}"
        elif self.level == "repository":
            path = f"projects/{self.project}/locations/{self.location}/repositories/{self.repository}"
        elif self.level == "image":
            path = f"projects/{self.project}/locations/{self.location}/repositories/{self.repository}/packages/{self.image}"
        elif self.level == "tag":
            path = f"projects/{self.project}/locations/{self.location}/repositories/{self.repository}/packages/{self.image}/tags/{self.tag}"
        elif self.level == "version":
            path = f"projects/{self.project}/locations/{self.location}/repositories/{self.repository}/packages/{self.image}/versions/sha256:{self.version}"
        if shorten:
            path = "/".join(path.split("/")[1::2])
        return path

    def _get_parent(self):
        """
        Get the parent resource. This is used for making requests to the API. For us this is either location or project.

        Returns:
        - str: The parent resource.
        """
        if self.location is not None:
            return f"projects/{self.project}/locations/{self.location}"
        elif self.project is not None:
            return f"projects/{self.project}"
        else:
            return ""

    def ls_repositories(self, full_id=False):
        """
        List repositories in the Artifact Registry for a given location.

        Args:
        - full_id (bool): Whether to return the full repository ID or just the name.

        Returns:
        - list: The list of repositories.
        """
        output = self.client.list_repositories(parent=self.parent)
        output = [repository.name for repository in output]
        if not full_id:
            output = [repository.split("/")[-1] for repository in output]
        return output

    def ls_files(self, repository=None):
        """
        List files in a repository.

        Args:
        - repository (str): The name of the repository.

        Returns:
        - list: The list of files.
        """
        repository = repository or self.repository
        parent = f"{self.parent}/repositories/{repository}"
        output = self.client.list_files(parent=parent)
        output = [file.name for file in output]
        return output

    def ls_images(self, repository=None, image=None, full_id=False):
        """
        List images in a repository.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - full_id (bool): Whether to return the full image ID or just the name.

        Returns:
        - list: The list of images.
        """
        repository = repository or self.repository
        image = image or self.image
        parent = f"{self.parent}/repositories/{repository}"
        output = self.client.list_packages(parent=parent)
        output = [package.name for package in output]
        if not full_id:
            # Careful not to split("/") in case there is a "/" in the package name
            images = [package.replace(f"{parent}/packages/", "") for package in output]
            output = [f"{repository}/{image}" for image in images]
        return output

    def ls_versions(self, repository=None, image=None, tag=None, full_id=False):
        """
        List versions in an image.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - tag (str): The tag of the image.
        - full_id (bool): Whether to return the full tag ID or just the name.

        Returns:
        - list: The list of tags, sorted by creation time (latest first)
        """
        repository = repository or self.repository
        image = image or self.image
        tag = tag or self.tag
        parent = f"{self.parent}/repositories/{repository}/packages/{image}"
        output = self.client.list_versions(parent=parent)
        output = [(tag.name, tag.create_time) for tag in output]
        output = sorted(output, key=lambda x: x[1], reverse=True)
        output = [tag[0] for tag in output]
        if not full_id:
            versions = [tag.replace(f"{parent}/versions/", "") for tag in output]
            output = [f"{repository}/{image}/{version}" for version in versions]
        return output

    def ls_tags(self, repository=None, image=None, version=None, full_id=False):
        """
        List tags in a version.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.
        - full_id (bool): Whether to return the full tag ID or just the name.

        Returns:
        - list: The list of tags.
        """
        repository = repository or self.repository
        image = image or self.image
        version = version or self.version
        parent = f"{self.parent}/repositories/{repository}/packages/{image}"
        output = self.client.list_tags(parent=parent)
        tag_version = f"{parent}/versions/sha256:{version}"
        output = [tag.name for tag in output if tag.version == tag_version]
        if not full_id:
            tags = [tag.replace(f"{parent}/tags/", "") for tag in output]
            output = [f"{repository}/{image}:{tag}" for tag in tags]
        return output

    def ls(self):
        """
        List repositories or files in a repository.

        Returns:
        - list: The list of repositories or files.
        """
        if self.level == "version":
            return self.ls_tags()
        elif self.level == "image":
            return self.ls_versions()
        elif self.level == "repository":
            return self.ls_images()
        elif self.level == "location":
            return self.ls_repositories()
        elif self.level == "project":
            raise ValueError("Cannot list items at the project level.")

    def get_repository(self, repository=None):
        """
        Get a repository.

        Args:
        - repository (str): The name of the repository.

        Returns:
        - Repository: The repository.
        """
        repository = repository or self.repository
        parent = f"{self.parent}/repositories/{repository}"
        return self.client.get_repository(name=parent)

    def get_image(self, repository=None, image=None):
        """
        Get an image.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.

        Returns:
        - Package: The image.
        """
        repository = repository or self.repository
        image = image or self.image
        parent = f"{self.parent}/repositories/{repository}/packages/{image}"
        return self.client.get_package(name=parent)

    def get_version(self, repository=None, image=None, version=None):
        """
        Get a version.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.

        Returns:
        - Version: The version.
        """
        repository = repository or self.repository
        image = image or self.image
        version = version or self.version
        parent = f"{self.parent}/repositories/{repository}/packages/{image}/versions/sha256:{version}"
        return self.client.get_version(name=parent)

    def get_version_from_tag(self, repository=None, image=None, tag=None):
        """
        Get a tag.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - tag (str): The tag of the image.

        Returns:
        - Tag: The tag.
        """
        repository = repository or self.repository
        image = image or self.image
        tag = tag or self.tag
        parent = f"{self.parent}/repositories/{repository}/packages/{image}/tags/{tag}"
        result = self.client.get_tag(name=parent)
        if result is None:
            raise ValueError(f"Tag not found in {self.location}/{repository}/{image}.")
        version = result.version
        sha256_version = version.replace(
            f"{self.parent}/repositories/{repository}/packages/{image}/versions/sha256:",
            "",
        )
        return sha256_version

    def get_from_tag(self, repository=None, image=None, tag=None):
        """
        Get the artifact from a tag.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - tag (str): The tag of the image.

        Returns:
        - Tag: The tag.
        """
        version = self.get_version_from_tag(repository, image, tag)
        output = self.get_version(repository, image, version)
        return output

    def get_tag(self, repository=None, image=None, tag=None):
        """
        Get a tag.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - tag (str): The tag of the image.

        Returns:
        - Tag: The tag.
        """
        repository = repository or self.repository
        image = image or self.image
        tag = tag or self.tag
        parent = f"{self.parent}/repositories/{repository}/packages/{image}/tags/{tag}"
        return self.client.get_tag(name=parent)

    def get(self):
        """
        Get an image or version.

        Returns:
        - Package or Version: The image or version.
        """
        if self.level == "repository":
            return self.get_repository()
        elif self.level == "image":
            return self.get_image()
        elif self.level == "version":
            return self.get_version()
        elif self.level == "tag":
            return self.get_from_tag()
        else:
            raise ValueError("Cannot get item at this level.")

    def exists(self):
        """
        Check if a repo/image/version/tag exists.

        Returns:
        - bool: Whether the image or version exists.
        """
        try:
            self.get()
            return True
        except self.NotFound:
            return False

    def delete_repository(self, repository=None, **kwargs):
        """
        Delete a repository.

        Args:
        - repository (str): The name of the repository.

        Returns:
        - None
        """
        repository = repository or self.repository
        parent = f"{self.parent}/repositories/{repository}"
        output = self.client.delete_repository(name=parent)
        output.result()
        log(f"Artifact Registry - Deleted repository {self.location}/{repository}.")
        return output

    def delete_image(self, repository=None, image=None, **kwargs):
        """
        Delete an image.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.

        Returns:
        - None
        """
        repository = repository or self.repository
        image = image or self.image
        parent = f"{self.parent}/repositories/{repository}/packages/{image}"
        output = self.client.delete_package(name=parent)
        output.result()
        log(f"Artifact Registry - Deleted image {self.location}/{repository}/{image}.")
        return output

    def delete_version(self, repository=None, image=None, version=None, **kwargs):
        """
        Delete a version.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.

        Returns:
        - None
        """
        repository = repository or self.repository
        image = image or self.image
        version = version or self.version
        parent = f"{self.parent}/repositories/{repository}/packages/{image}/versions/sha256:{version}"
        output = self.client.delete_version(name=parent)
        try:
            output.result()
        except self.FailedPrecondition as e:
            if "because it is tagged." in str(e):
                log("Artifact Registry - Version is tagged. Deleting tags first...")
                tags = self.ls_tags(repository, image, version)
                self._delete_parallel(tags)
                output = self.client.delete_version(name=parent)
        log(
            f"Artifact Registry - Deleted version {self.location}/{repository}/{image}/sha256:{version}"
        )
        return output

    def delete_tag(self, tag=None, repository=None, image=None, **kwargs):
        """
        Delete a tag.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - tag (str): The tag of the image.

        Returns:
        - None
        """
        repository = repository or self.repository
        image = image or self.image
        tag = tag or self.tag
        parent = f"{self.parent}/repositories/{repository}/packages/{image}/tags/{tag}"
        output = self.client.delete_tag(name=parent)
        try:
            output.result()
        except AttributeError:
            pass
        log(
            f"Artifact Registry - Deleted tag {self.location}/{repository}/{image}:{tag}."
        )
        return output

    def delete(
        self, repository=None, image=None, version=None, tag=None, errors="ignore"
    ):
        """
        Delete a repository, image, version, or a tag.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.
        - tag (str): The tag of the image.
        - errors (str): How to handle errors if the item is not found. Can be either 'ignore' or 'raise'.

        Returns:
        - None
        """
        if self.level == "repository":
            operation = self.delete_repository
        elif self.level == "image":
            operation = self.delete_image
        elif self.level == "version":
            operation = self.delete_version
        elif self.level == "tag":
            operation = self.delete_tag
        else:
            raise ValueError("Cannot delete item at this level.")
        try:
            output = operation(
                repository=repository, image=image, version=version, tag=tag
            )
        except self.NotFound as e:
            if errors == "ignore":
                log(f"Artifact Registry - WARNING: {e}")
            else:
                raise e

    def _delete_parallel(self, items):
        """
        Deletes items in parallel using threading.

        Args:
        - items (list): List of items to delete.
        """
        threads = []
        for item in items:
            thread = threading.Thread(
                target=lambda item=item: ArtifactRegistry(
                    path=item, location=self.location
                ).delete()
            )
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def create_repository(
        self,
        name=None,
        format="docker",
        mode="standard",
        immutable_tags=False,
        version_policy=None,
    ):
        """
        Create a repository.

        Args:
        - name (str): The name of the repository. If not provided, the repository name from the constructor will be used.
        - format (str): The format of the repository. Can be either 'docker' or 'maven' (default: 'docker')
        - mode (str): The mode of the repository. One of 'standard', 'remote' or 'virtual' (default: 'standard')
        - immutable_tags (bool): [Docker only] Whether to use immutable tags (default: False)
        - version_policy (str): [Maven only] The version policy for the repository - None, Snapshop or Release (default: None)



        Returns:
        - Repository: The repository.
        """
        maven_config = None
        docker_config = None
        repository = name or self.repository
        parent = f"{self.parent}/repositories/{repository}"
        if format == "docker":
            docker_config = self.types.Repository.DockerRepositoryConfig(
                immutable_tags=immutable_tags
            )
        elif format == "maven":
            if isinstance(version_policy, str):
                version_policy = version_policy.lower()
            version_policy = {None: 0, "none": 0, "snapshot": 1, "release": 2}[
                version_policy
            ]
            version_policy = self.types.Repository.MavenRepositoryConfig.VersionPolicy(
                version_policy
            )
            maven_config = self.types.Repository.MavenRepositoryConfig(
                version_policy=version_policy
            )
        mode = {"standard": 1, "remote": 2, "virtual": 3}.get(mode.lower(), None)
        if mode is None:
            raise ValueError("Mode must be either 'standard', 'remote' or 'virtual'.")
        format = {"docker": 1, "maven": 2}.get(format.lower(), None)
        if format is None:
            raise ValueError("Format must be either 'docker' or 'maven'.")
        format = self.types.Repository.Format(format)
        mode = self.types.Repository.Mode(mode)
        repository = self.types.Repository(
            name=parent,
            format_=format,
            mode=mode,
            docker_config=docker_config,
            maven_config=maven_config,
        )
        output = self.client.create_repository(
            parent=self.parent, repository=repository, repository_id=self.repository
        )
        try:
            output.result()
        except AttributeError:
            pass
        log(
            f"Artifact Registry - Created repository {self.project}/{self.location}/{repository}."
        )
        return output

    def create_tag(self, tag=None, repository=None, image=None, version=None):
        """
        Create a tag.

        Args:
        - tag (str): The tag to add to the version of the image.
        - value (str): The value of the tag.
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.

        Returns:
        - Tag: The tag.
        """
        repository = repository or self.repository
        image = image or self.image
        version = version or self.version
        tag = tag or self.tag
        tag_name = tag
        Tag = self.types.Tag
        package_id = f"{self.parent}/repositories/{repository}/packages/{image}"
        version_id = f"{package_id}/versions/sha256:{version}"
        tag_id = f"{package_id}/tags/{tag}"
        tag = Tag(name=tag_id, version=version_id)
        output = self.client.create_tag(parent=package_id, tag=tag, tag_id=tag_name)
        log(
            f"Artifact Registry - Created tag {self.location}/{repository}/{image}:{tag}."
        )
        return output

    def create(self, repository=None, image=None, version=None, tag=None, **kwargs):
        """
        Create a repository, image, version, or a tag.

        Args:
        - repository (str): The name of the repository.
        - image (str): The name of the image.
        - version (str): The version of the image.
        - tag (str): The tag of the image.
        - kwargs: Additional arguments to pass to the creation function.

        Returns:
        - Repository, Package, Version, or Tag: The repository, image, version, or tag.
        """
        inputs = {
            "repository": repository,
            "image": image,
            "version": version,
            "tag": tag,
        }
        if self.level == "repository":
            return self.create_repository(**inputs, **kwargs)
        elif self.level == "image":
            raise ValueError("Creating images from the API is not supported.")
        elif self.level == "version":
            raise ValueError("Creating versions from the API is not supported.")
        elif self.level == "tag":
            return self.create_tag(**inputs, **kwargs)
        else:
            raise ValueError("Cannot create item at this level.")


if __name__ == "__main__":
    ArtifactRegistry(
        "gcr.io/example-service-123/sha256:411f06abcbda4b36a77c6e792e699b4eeb0193ebe441b6144f8fe42db6eada47",
        location="us",
    ).create_tag("latest_123")
