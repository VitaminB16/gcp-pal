import os
import gcsfs

from gcp_tools.utils import get_default_project, log


class Storage:
    """
    Class for operating Google Cloud Storage
    """

    _clients = {}

    def __init__(
        self,
        path=None,
        bucket_name=None,
        project=None,
        location="europe-west2",
    ):
        self.project = project or os.environ.get("PROJECT") or get_default_project()
        self.location = location
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

        self.base_path = self.path[len(self.fs_prefix) :]
        self.file_name = self.base_path[len(self.bucket_name) + 1 :]
        self.file_name = self.file_name if self.file_name != "" else None

        if self.bucket_name == "":
            self.bucket_name = None

        self.ref_type = self._ref_type()

        if self.project in Storage._clients:
            self.fs = Storage._clients[self.project]
        else:
            self.fs = gcsfs.GCSFileSystem(
                project=self.project,
                default_location=self.location,
                cache_timeout=0,
            )
            Storage._clients[self.project] = self.fs

    def __repr__(self):
        """
        String representation of the class.

        Examples:
        - `Storage("bucket_name")` -> `Storage(gs://bucket_name)`
        """
        return f"Storage({self.path})"

    def _ref_type(self):
        """
        Get the reference type of the path.

        Returns:
        - str: Reference type of the path
        """
        self.is_file, self.is_bucket, self.is_project = False, False, False
        if not self.file_name and not self.bucket_name:
            self.is_project = True
            return "project"
        elif not self.file_name:
            self.is_bucket = True
            return "bucket"
        elif self.file_name:
            self.is_file = True
            return "file"
        raise ValueError("Invalid path")

    def ls(self, refresh=True):
        """
        List all files in the given path, or list all buckets in the project.

        Returns:
        - list: List of files in the bucket or list of buckets in the project.
        """
        if refresh:
            # It is not yet certain whether this will cause issues in large projects
            self.fs.invalidate_cache()
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

    def glob(self, query=None):
        """
        List all files or directories matching the path regex.

        Args:
        - query (str): Path/query regex to match. Can also be provided in self.path.

        Returns:
        - list: List of matching files or directories.

        Examples:
        - `Storage("bucket_name").glob()` -> List all files in the bucket
        - `Storage("bucket_name/*/*").glob()` -> List all files in the bucket with two subdirectories
        """
        if query:
            return self.fs.glob(query)
        if not self.bucket_name:
            return self.ls()
        elif self.path.startswith(self.fs_prefix + "*"):
            raise ValueError("Wildcard is not supported for the bucket name yet.")
        return self.fs.glob(self.path)

    def exists(self, path=None):
        """
        Check if the file or directory exists in the path.

        Returns:
        - bool: Whether the file or directory exists.
        """
        path = self._suffix_path(path)
        output = self.fs.exists(path)
        return output

    def mkdir(self, path=None, exist_ok=True):
        """
        Create a directory in the path.

        Args:
        - exist_ok (bool): Whether to ignore the error if the directory already exists.
        """
        path = self._suffix_path(path)
        output = self.fs.mkdir(path, exist_ok=exist_ok)
        log(f"Created directory {path}")
        return output

    def mkdirs(self, path=None, exist_ok=True):
        """
        Create directories recursively in the path.

        Args:
        - exist_ok (bool): Whether to ignore the error if the directory already exists.
        """
        if not path:
            path = self.path
        elif self.path.endswith("/"):
            path = self.path + path
        else:
            path = self.path + "/" + path
        output = self.fs.mkdirs(path, exist_ok=exist_ok)
        log(f"Created directories {path}")
        return output

    def create_bucket(self, bucket_name=None):
        """
        Create a bucket in the project.

        Args:
        - bucket_name (str): Name of the bucket to create.
        """
        if not bucket_name:
            bucket_name = self.bucket_name
        if not bucket_name:
            raise ValueError("Bucket name is required.")
        output = self.fs.mkdir(f"gs://{bucket_name}")
        log(f"Created bucket {bucket_name}")
        return output

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
        """
        output = self.fs.rm(self.path, recursive=recursive)
        log(f"Removed {self.path}")
        return output

    def rmdir(self, path=None):
        """
        Remove the directory in the path.

        Args:
        - path (str): Optional path from base path to remove.
        """
        path = self._suffix_path(path)
        path = self.bucket_name if self.is_bucket else path
        output = self.fs.rmdir(path)
        log(f"Removed {path}")
        return output

    def remove(self, recursive=True):
        """
        Alias for `rm`.
        """
        return self.rm(recursive=recursive)

    def delete(self):
        """
        Deletes the file or directory in the path. If the path is a bucket, it deletes the bucket.

        Args:
        - recursive (bool): Whether to delete the directory recursively.
        """
        path = self.path
        if path.endswith("/"):
            path = path[:-1]
        try:
            output = self.rm()
        except FileNotFoundError:
            output = self.rmdir()
        return output

    def copy(self, destination_path, recursive=True):
        """
        Copy a file or directory from the source to the destination.

        Args:
        - destination_path (str): Destination path
        - recursive (bool): Whether to copy the directory recursively.
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
        """
        output = self.fs.move(self.path, destination_path, recursive=recursive)
        log(f"Moved {self.path} to {destination_path}")
        return output

    def upload(self, local_path, recursive=True):
        """
        Upload a file or directory from the local path to the storage path.

        Args:
        - local_path (str): Local path
        - recursive (bool): Whether to upload the directory recursively.
        """
        output = self.fs.put(local_path, self.path, recursive=recursive)
        log(f"Uploaded {local_path} to {self.path}")
        return output

    def put(self, local_path, recursive=True):
        """
        Alias for `upload`.
        """
        return self.upload(local_path, recursive=recursive)

    def open(self, mode="r"):
        """
        Open the storage path in the given mode.

        Args:
        - mode (str): Mode to open the file in

        Returns:
        - file: File object
        """
        return self.fs.open(self.path, mode)

    def _suffix_path(self, path):
        """
        Append the path to the base path.

        Args:
        - path (str): Path to append

        Returns:
        - str: Suffixed path

        Examples:
        >>> Storage().suffix_path("file") -> "gs://file"
        >>> Storage("bucket_name").suffix_path("file") -> "gs://bucket_name/file"
        >>> Storage("bucket_name/path").suffix_path() -> "gs://bucket_name"
        """
        if not path:
            path = self.path
        elif self.path.endswith("/"):
            path = self.path + path
        else:
            path = self.path + "/" + path
        return path


if __name__ == "__main__":
    exit()


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
    print(Storage().project)
    print(Storage().ls())
    print(Storage().glob())
