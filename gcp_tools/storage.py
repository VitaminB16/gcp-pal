import os
import gcsfs

from gcp_tools.utils import get_default_project


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
