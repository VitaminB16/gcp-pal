import os
import gcsfs

from gcp_tools.utils import get_default_project, log


class Storage:
    """
    Class for operating Google Cloud Storage
    """

    _clients = {}

    def __init__(self, path=None, bucket_name=None, project=None):
        self.project = project or os.environ.get("PROJECT") or get_default_project()
        self.fs_prefix = "gs://"

        self.path = path or self.fs_prefix
        if not self.path.startswith(self.fs_prefix):
            self.path = self.fs_prefix + self.path

        self.bucket_name = bucket_name
        if not self.bucket_name:
            self.bucket_name = self.path[len(self.fs_prefix) :].split("/")[0]
        else:
            if not path or path == self.fs_prefix:
                self.path = self.path + self.bucket_name

        if self.bucket_name == "":
            self.bucket_name = None

        if self.project in Storage._clients:
            self.fs = Storage._clients[self.project]
        else:
            self.fs = gcsfs.GCSFileSystem(project=self.project)
            Storage._clients[self.project] = self.fs

    def __repr__(self):
        return f"Storage({self.path})"

    def ls(self):
        """
        List all files in the given path, or list all buckets in the project.

        Returns:
        - list: List of files in the bucket or list of buckets in the project.
        """
        return self.fs.ls(self.path)

    def download(self, local_path):
        """
        Download a file from the storage path to a local path.

        Args:
        - local_path (str): Local path where the file will be downloaded.
        """
        output = self.fs.get(self.path, local_path)
        log(f"Downloaded {self.path} to {local_path}")
        return output

    def glob(self):
        """
        List all files or directories matching the path regex.

        Returns:
        - list: List of matching files or directories.

        Examples:
        - `Storage("bucket_name").glob()` -> List all files in the bucket
        - `Storage("bucket_name/*/*").glob()` -> List all files in the bucket with two subdirectories
        """
        if self.path.startswith(self.fs_prefix + "*"):
            raise ValueError("Wildcard is not supported for the bucket name yet.")
        return self.fs.glob(self.path)

    def mkdir(self, exist_ok=True):
        """
        Create a directory in the path.

        Args:
        - exist_ok (bool): Whether to ignore the error if the directory already exists.

        Returns:
        - bool: Whether the directory was created successfully.
        """
        return self.fs.mkdir(self.path, exist_ok=exist_ok)

    def mkdirs(self, exist_ok=True):
        """
        Create directories recursively in the path.

        Args:
        - exist_ok (bool): Whether to ignore the error if the directory already exists.

        Returns:
        - bool: Whether the directories were created successfully.
        """
        return self.fs.mkdirs(self.path, exist_ok=exist_ok)

    def walk(self):
        """
        Walk through the directory tree starting from the path.
        """
        return self.fs.walk(self.path)

    def rm(self, recursive=True):
        """
        Remove the file or directory in the path.

        Args:
        - recursive (bool): Whether to remove the directory recursively.

        Returns:
        - bool: Whether the removal was successful.
        """
        return self.fs.rm(self.path, recursive=recursive)

    def remove(self, recursive=True):
        """
        Alias for `rm`.
        """
        return self.rm(recursive=recursive)

    def copy(self, destination_path, recursive=True):
        """
        Copy a file or directory from the source to the destination.

        Args:
        - destination_path (str): Destination path
        - recursive (bool): Whether to copy the directory recursively.

        Returns:
        - bool: Whether the copy was successful.
        """
        output = self.fs.copy(self.path, destination_path, recursive=recursive)
        log(f"Copied {self.path} to {destination_path}")
        return output

    def move(self, destination_path, recursive=True):
        """
        Move a file or directory from the source to the destination.

        Args:
        - destination_path (str): Destination path
        - recursive (bool): Whether to move the directory recursively.

        Returns:
        - bool: Whether the move was successful.
        """
        output = self.fs.move(self.path, destination_path, recursive=recursive)
        log(f"Moved {self.path} to {destination_path}")
        return output


if __name__ == "__main__":
    print(Storage("bucket_name").bucket_name == "bucket_name")
    print(Storage("gs://bucket_name").bucket_name == "bucket_name")
    print(Storage("bucket_name/file").bucket_name == "bucket_name")
    print(Storage("gs://bucket_name/file").bucket_name == "bucket_name")
    print(Storage("gs://bucket_name").path == "gs://bucket_name")
    print(Storage("bucket_name").path == "gs://bucket_name")
    print(Storage("bucket_name/file").path == "gs://bucket_name/file")
    print(Storage("gs://bucket_name/file").path == "gs://bucket_name/file")
    print(Storage("gs://bucket_name/file").path == "gs://bucket_name/file")
    print(Storage(bucket_name="bucket").bucket_name == "bucket")
    print(Storage(bucket_name="bucket").path == "gs://bucket")
    print(Storage().path == "gs://")
    print(Storage().bucket_name is None)
