import gcsfs
from uuid import uuid4
from gcp_tools import Storage


# Utilities which do not depend on Storage class
def create_bucket(bucket_name):
    fs = gcsfs.GCSFileSystem()
    fs.mkdir(f"gs://{bucket_name}")


def list_buckets():
    fs = gcsfs.GCSFileSystem()
    fs.invalidate_cache()
    return fs.ls("gs://")


def list_files(bucket_name):
    fs = gcsfs.GCSFileSystem()
    fs.invalidate_cache()
    return fs.ls(f"gs://{bucket_name}")


def create_file(bucket_name, file_name):
    fs = gcsfs.GCSFileSystem()
    fs.invalidate_cache()
    fs.touch(f"gs://{bucket_name}/{file_name}")


# Tests for the Storage class
def test_storage_init():
    assert Storage("bucket_name").bucket_name == "bucket_name"
    assert Storage("gs://bucket_name").bucket_name == "bucket_name"
    assert Storage("bucket_name/file").bucket_name == "bucket_name"
    assert Storage("gs://bucket_name/file").bucket_name == "bucket_name"
    assert Storage("gs://bucket_name").path == "gs://bucket_name"
    assert Storage("bucket_name").path == "gs://bucket_name"
    assert Storage("bucket_name/file").path == "gs://bucket_name/file"
    assert Storage("gs://bucket_name/file").path == "gs://bucket_name/file"
    assert Storage(bucket_name="bucket").bucket_name == "bucket"
    assert Storage(bucket_name="bucket").path == "gs://bucket"
    assert Storage().path == "gs://"
    assert Storage().bucket_name is None
    assert Storage("path").fs_prefix == "gs://"
    assert Storage("gs://").ref_type == "project"
    assert Storage("gs://bucket").ref_type == "bucket"
    assert Storage("gs://bucket/file").ref_type == "file"
    assert Storage("gs://bucket").is_bucket
    assert Storage("gs://bucket/file").is_file
    assert Storage("gs://").is_project
    assert Storage("gs://bucket/filepath").base_path == "bucket/filepath"
    assert Storage("gs://bucket/filepath").file_name == "filepath"


def test_storage_ls_glob():
    success = {}
    # Test bucket ls
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    success[0] = bucket_name + "/" in Storage().ls()
    # Test file ls
    file_name = f"test_file_{uuid4()}"
    create_file(bucket_name, file_name)
    success[1] = f"{bucket_name}/{file_name}" in Storage(bucket_name).ls()
    # Test file ls with path
    success[2] = f"{bucket_name}/{file_name}" in Storage(f"gs://{bucket_name}").ls()

    # Test bucket glob
    success[3] = bucket_name + "/" in Storage().glob()

    # Test file glob
    glob_query = f"{bucket_name}/*"
    success[4] = f"{bucket_name}/{file_name}" in Storage(glob_query).glob()
    success[5] = f"{bucket_name}/{file_name}" in Storage().glob(glob_query)

    # Test subdirectory file glob
    subdirectory = f"subdirectory"
    file_path = f"{bucket_name}/{subdirectory}/{file_name}"
    create_file(bucket_name, f"{subdirectory}/{file_name}")
    glob_query = f"{bucket_name}/*/*"
    print(Storage(glob_query).glob())
    success[6] = file_path in Storage(glob_query).glob()

    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_mkdir():
    success = {}
    # Test bucket mkdir
    bucket_name = f"test_bucket_{uuid4()}"
    Storage(bucket_name).mkdir()
    success[0] = bucket_name + "/" in list_buckets()

    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_delete():
    success = {}
    # Test bucket delete
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    success[0] = bucket_name + "/" in list_buckets()
    Storage(bucket_name).delete()
    success[1] = bucket_name + "/" not in list_buckets()

    # Test file delete
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    file_name = f"test_file_{uuid4()}"
    create_file(bucket_name, file_name)
    print(bucket_name)
    print(list_files(bucket_name))
    success[2] = f"{bucket_name}/{file_name}" in list_files(bucket_name)
    Storage(f"{bucket_name}/{file_name}").delete()
    success[3] = f"{bucket_name}/{file_name}" not in list_files(bucket_name)

    # Test bucket delete with files
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    file_name = f"test_file_{uuid4()}"
    create_file(bucket_name, file_name)
    success[4] = f"{bucket_name}/{file_name}" in list_files(bucket_name)
    Storage(bucket_name).delete()
    success[5] = bucket_name + "/" not in list_buckets()

    failed = [k for k, v in success.items() if not v]
    assert not failed
