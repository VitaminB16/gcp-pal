import os
from gcp_pal.utils import try_import


from gcp_pal.utils import get_auth_default, log, ClientHandler, ModuleHandler


class Storage:
    """
    Class for operating Google Cloud Storage
    """

    def __init__(
        self,
        path=None,
        bucket_name=None,
        project=None,
        location="europe-west2",
    ):
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
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
        self.name = self.file_name or self.bucket_name

        self.bucket_path = self.fs_prefix + self.bucket_name
        if self.bucket_name == "":
            self.bucket_name = None
            self.bucket_path = None

        self.ref_type = self._ref_type()
        self.gcsfs = ModuleHandler("gcsfs").please_import(who_is_calling="Storage")
        self.GCSFileSystem = self.gcsfs.GCSFileSystem
        self.fs = ClientHandler(self.GCSFileSystem).get(
            project=self.project,
            default_location=self.location,
            cache_timeout=0,
        )

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

    def download(self, local_path=None):
        """
        Download a file from the storage path to a local path.

        Args:
        - local_path (str): Local path where the file will be downloaded.
        """
        if not local_path:
            output = self.read()
        else:
            output = self.fs.get(self.path, local_path)
            log(f"Storage - Downloaded: {self.path} -> {local_path}")
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
        self.fs.invalidate_cache()
        path = self._suffix_path(path)
        output = self.fs.exists(path)
        return output

    def isdir(self, path=None):
        """
        Check if the path is a directory.

        Returns:
        - bool: Whether the path is a directory.
        """
        self.fs.invalidate_cache()
        path = self._suffix_path(path)
        output = self.fs.isdir(path)
        try:
            object_info = self.fs.info(path)
        except FileNotFoundError:
            return output
        metadata = object_info.get("metadata", {})
        if object_info.get("type", "") == "directory":
            output = True
        elif metadata.get("arrow/gcsfs", "") == "directory":
            output = True
        elif object_info.get("name", "").endswith("/"):
            output = True
        return output

    def isfile(self, path=None):
        """
        Check if the path is a file.

        Returns:
        - bool: Whether the path is a file.
        """
        self.fs.invalidate_cache()
        path = self._suffix_path(path)
        output = self.fs.isfile(path)
        try:
            object_info = self.fs.info(path)
        except FileNotFoundError:
            return output
        metadata = object_info.get("metadata", {})
        if object_info.get("type", "") == "directory":
            output = False
        elif metadata.get("arrow/gcsfs", "") == "directory":
            output = False
        elif object_info.get("name", "").endswith("/"):
            output = False
        return output

    def mkdir(self, path=None, exist_ok=True):
        """
        Create a directory in the path.

        Args:
        - exist_ok (bool): Whether to ignore the error if the directory already exists.
        """
        path = self._suffix_path(path)
        output = self.fs.mkdir(path, exist_ok=exist_ok)
        log(f"Created directory: {path}")
        return output

    def mkdirs(self, path=None, exist_ok=True):
        """
        Create directories recursively in the path.

        Args:
        - exist_ok (bool): Whether to ignore the error if the directory already exists.
        """
        path = self._suffix_path(path)
        output = self.fs.mkdirs(path, exist_ok=exist_ok)
        log(f"Storage - Created directories: {path}")
        return output

    def create_bucket(self, bucket_name=None, exists_ok=True):
        """
        Create a bucket in the project.

        Args:
        - bucket_name (str): Name of the bucket to create.
        """
        if not bucket_name:
            bucket_name = self.bucket_name
        if not bucket_name:
            raise ValueError("Bucket name is required.")
        try:
            output = self.fs.mkdir(bucket_name)
        except self.gcsfs.retry.HttpError as e:
            exists = "Your previous request to create the named bucket succeeded and you already own it."
            if exists in str(e) and exists_ok:
                return
            raise e
        log(f"Storage - Created bucket: {bucket_name}")
        return output

    def create(self, path=None, exist_ok=True, bucket_name=None):
        """
        Alias for `create_bucket` or `mkdir`.

        Args:
        - path (str): Path to create
        - exist_ok (bool): Whether to ignore the error if the directory already exists.
        - bucket_name (str): Name of the bucket to create.
        """
        if bucket_name or self.is_bucket:
            return self.create_bucket(bucket_name, exists_ok=exist_ok)
        return self.mkdir(path, exist_ok=exist_ok)

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
        log(f"Storage - Removed: {self.path}")
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
        log(f"Storage - Removed: {path}")
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
        log(f"Storage - Copied: {self.path} -> {destination_path}")
        return output

    def move(self, destination_path, recursive=True):
        """
        Move a file or directory from the source to the destination.

        Args:
        - destination_path (str): Destination path
        - recursive (bool): Whether to move the directory recursively.
        """
        output = self.fs.move(self.path, destination_path, recursive=recursive)
        log(f"Storage - Moved: {self.path} -> {destination_path}")
        return output

    def _upload(self, local_path=None, contents=None, recursive=True):
        """
        Upload a file or directory from the local path to the storage path.

        Args:
        - local_path (str): Local path from which to upload the file or directory.
        - contents (str): Contents to upload.
        - recursive (bool): Whether to upload the directory recursively.
        """
        if local_path:
            try:
                output = self.fs.put(local_path, self.path, recursive=recursive)
            except FileNotFoundError as e:
                self.create_bucket()
                output = self.fs.put(local_path, self.path, recursive=recursive)
            log(f"Storage - Uploaded: {local_path} -> {self.path}")
        elif contents:
            if isinstance(contents, str):
                mode = "w"
            else:
                mode = "wb"
            output = self.write(contents, mode=mode)
        return output

    def upload(self, local_path=None, contents=None, recursive=True):
        """
        Upload a file or directory from the local path to the storage path.

        Args:
        - local_path (str): Local path from which to upload the file or directory.
        - contents (str): Contents to upload.
        - recursive (bool): Whether to upload the directory recursively.
        """
        try:
            output = self._upload(local_path, contents, recursive)
        except FileNotFoundError as e:
            self.create_bucket()
            output = self._upload(local_path, contents, recursive)
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
        - mode (str): Mode with which to open the file

        Returns:
        - file: File object
        """
        self.fs.invalidate_cache()
        return self.fs.open(self.path, mode=mode)

    def _suffix_path(self, path=None):
        """
        Append the path to the base path. Will not append if the path already starts with the fs_prefix.

        Args:
        - path (str): Path to append

        Returns:
        - str: Suffixed path

        Examples:
        >>> Storage()._suffix_path("file") -> "gs://file"
        >>> Storage("bucket_name")._suffix_path("file") -> "gs://bucket_name/file"
        >>> Storage("bucket_name/path")._suffix_path("gs://bucket_name") -> "gs://bucket_name/path"
        >>> Storage("bucket_name/path")._suffix_path() -> "gs://bucket_name"
        """
        if path is None:
            return self.path
        if path.startswith(self.fs_prefix):
            return path
        if path.startswith("/"):
            path = path[1:]
        if self.path.endswith("/"):
            return self.path + path
        return self.path + "/" + path

    def _write(self, data, path=None, mode="w", **kwargs):
        """
        Write data to the path.

        Args:
        - data: Data to write
        - path (str): Path to which to write the data
        - mode (str): Mode with which to write the file
        - kwargs: Additional arguments to pass to the write method
        """
        path = self._suffix_path(path)
        if path.endswith(".parquet") or path.endswith(".parquet/"):
            from gcp_pal import Parquet

            return Parquet(path).write(data, **kwargs)
        with self.fs.open(path, mode=mode) as f:
            f.write(data, **kwargs)
        log(f"Storage - Written: {path}")

    def write(self, data, path=None, mode="w", **kwargs):
        """
        Write data to the path.

        Args:
        - data: Data to write
        - path (str): Path to which to write the data
        - mode (str): Mode with which to write the file
        - kwargs: Additional arguments to pass to the write method
        """
        try:
            self._write(data, path, mode, **kwargs)
        except FileNotFoundError as e:
            self.create_bucket()
            self._write(data, path, mode, **kwargs)

    def read(self, path=None, **kwargs):
        """
        Read data from the path.

        Args:
        - path (str): Path to read
        - kwargs: Additional arguments to pass to the read

        Returns:
        - str: Data read from the path
        """
        self.fs.invalidate_cache()
        path = self._suffix_path(path)
        if path.endswith(".parquet") or path.endswith(".parquet/"):
            from gcp_pal import Parquet

            return Parquet(path).read(**kwargs)
        with self.fs.open(path, mode="r") as f:
            data = f.read()
        log(f"Storage - Read: {path}")
        return data

    def get(self, path=None, **kwargs):
        """
        Gets the object from the path as a reference.
        """
        try_import("google.cloud.storage", "Storage.get")
        from google.cloud.storage.blob import Blob
        from google.cloud.storage.bucket import Bucket

        path = self._suffix_path(path)
        bucket_name = path[len(self.fs_prefix) :].split("/")[0]
        path = path[len(bucket_name) + 1 :]
        bucket = Bucket(bucket_name, self.project)
        return Blob(path, bucket)


# if __name__ == "__main__":
#     import pyarrow as pa
#     import pyarrow.parquet as pq
#     import pandas as pd

#     success = {}
#     bucket_name = f"test_bucket_vita_1324"
#     Storage(bucket_name).create()

#     df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
#     partition_cols = ["a"]
#     file_name = f"gs://{bucket_name}/file.parquet"
#     table = pa.Table.from_pandas(df)
#     pq.write_to_dataset(
#         table, file_name, partition_cols=partition_cols, basename_template="{i}.parquet"
#     )
#     print(file_name)
#     print(Storage(file_name).isfile())
#     print(Storage(file_name).isfile("a=1"))
#     exit()

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
