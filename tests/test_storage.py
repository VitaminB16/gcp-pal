import os
import gcsfs
from uuid import uuid4
from gcp_pal import Storage


# Utilities which do not depend on Storage class
def create_bucket(bucket_name):
    fs = gcsfs.GCSFileSystem()
    fs.mkdir(f"gs://{bucket_name}", location="europe-west2")


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
    with fs.open(f"gs://{bucket_name}/{file_name}", "w") as f:
        f.write("abc")


# Tests for the Storage class
def test_storage_init():
    success = {}
    success[0] = Storage("bucket_name").bucket_name == "bucket_name"
    success[1] = Storage("gs://bucket_name").bucket_name == "bucket_name"
    success[2] = Storage("bucket_name/file").bucket_name == "bucket_name"
    success[3] = Storage("gs://bucket_name/file").bucket_name == "bucket_name"
    success[4] = Storage("gs://bucket_name").path == "gs://bucket_name"
    success[5] = Storage("bucket_name").path == "gs://bucket_name"
    success[6] = Storage("bucket_name/file").path == "gs://bucket_name/file"
    success[7] = Storage("gs://bucket_name/file").path == "gs://bucket_name/file"
    success[8] = Storage(bucket_name="bucket").bucket_name == "bucket"
    success[9] = Storage(bucket_name="bucket").path == "gs://bucket"
    success[10] = Storage().path == "gs://"
    success[11] = Storage().bucket_name is None
    success[12] = Storage("path").fs_prefix == "gs://"
    success[13] = Storage("gs://").ref_type == "project"
    success[14] = Storage("gs://bucket").ref_type == "bucket"
    success[15] = Storage("gs://bucket/file").ref_type == "file"
    success[16] = Storage("gs://bucket").is_bucket
    success[17] = Storage("gs://bucket/file").is_file
    success[18] = Storage("gs://").is_project
    success[19] = Storage("gs://bucket/filepath").base_path == "bucket/filepath"
    success[20] = Storage("gs://bucket/filepath").file_name == "filepath"

    failed = [k for k, v in success.items() if not v]

    assert not failed


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


def test_create_bucket():
    success = {}
    # Test bucket create
    bucket_name = f"test_bucket_{uuid4()}"
    Storage(bucket_name).create_bucket()
    success[0] = bucket_name + "/" in list_buckets()

    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_create():
    success = {}
    # Test bucket create
    bucket_name = f"test_bucket_{uuid4()}"
    Storage(bucket_name).create()
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


def test_upload_download():
    success = {}
    # Test file upload
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    file_name = f"tests/test_storage.py"
    Storage(f"{bucket_name}/{file_name}").upload(file_name)
    success[0] = f"{bucket_name}/{file_name}" in list_files(bucket_name + "/tests")

    # Test file download
    download_path = f"tests_output/test_storage_download.py"
    Storage(f"{bucket_name}/{file_name}").download(download_path)
    success[1] = os.path.exists(download_path)
    success[2] = os.path.getsize(download_path) == os.path.getsize(file_name)

    Storage(bucket_name).delete()

    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_copy_move():
    success = {}
    # Test file copy
    start_bucket = f"test_bucket_{uuid4()}"
    end_bucket = f"test_bucket_{uuid4()}"
    print(start_bucket)
    print(end_bucket)
    create_bucket(start_bucket)
    create_bucket(end_bucket)
    file_name = f"test_file_{uuid4()}"
    create_file(start_bucket, file_name)

    # Test copy
    Storage(f"{start_bucket}/{file_name}").copy(f"{end_bucket}/{file_name}_copy")
    success[0] = f"{end_bucket}/{file_name}_copy" in list_files(end_bucket)
    success[1] = f"{start_bucket}/{file_name}" in list_files(start_bucket)

    # Test move
    Storage(f"{start_bucket}/{file_name}").move(f"{end_bucket}/{file_name}_move")
    success[2] = f"{end_bucket}/{file_name}_move" in list_files(end_bucket)
    success[3] = f"{start_bucket}/{file_name}" not in list_files(start_bucket)

    failed = [k for k, v in success.items() if not v]

    Storage(start_bucket).delete()
    Storage(end_bucket).delete()
    assert not failed


def test_open():
    success = {}
    # Test file open
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    file_name = f"test_file_{uuid4()}"
    create_file(bucket_name, file_name)

    with Storage(f"{bucket_name}/{file_name}").open() as f:
        success[0] = f.read() == "abc"

    failed = [k for k, v in success.items() if not v]

    Storage(bucket_name).delete()
    assert not failed


def test_exists():
    success = {}
    # Test file exists
    bucket_name = f"test_bucket_{uuid4()}"

    success[0] = not Storage(bucket_name).exists()
    create_bucket(bucket_name)
    success[1] = Storage(bucket_name).exists()

    file_name = f"test_file_{uuid4()}"

    success[2] = not Storage(f"{bucket_name}/{file_name}").exists()
    success[3] = not Storage(f"{bucket_name}").exists(file_name)
    create_file(bucket_name, file_name)
    success[4] = Storage(f"{bucket_name}/{file_name}").exists()
    success[5] = Storage(f"{bucket_name}").exists(file_name)

    success[6] = not Storage(f"{bucket_name}/{file_name}_not_exists").exists()
    success[7] = not Storage(f"{bucket_name}").exists(f"{file_name}_not_exists")

    failed = [k for k, v in success.items() if not v]

    Storage(bucket_name).delete()
    assert not failed


def test_suffix_path():
    success = {}
    success[0] = Storage()._suffix_path("file") == "gs://file"
    success[1] = Storage("bucket_name")._suffix_path("file") == "gs://bucket_name/file"
    success[2] = Storage("bucket_name/path")._suffix_path() == "gs://bucket_name/path"

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_is_directory():
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    success = {}
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    success[0] = Storage(bucket_name).isdir()
    success[1] = not Storage(f"{bucket_name}/file").isdir()

    create_file(bucket_name, "folder/folder/file.txt")
    success[2] = Storage(f"{bucket_name}/folder").isdir()
    success[3] = Storage(f"{bucket_name}").isdir("folder")
    success[4] = Storage(f"{bucket_name}/folder").isdir("folder")
    success[5] = Storage(f"{bucket_name}/").isdir("folder/folder")
    success[6] = not Storage(f"{bucket_name}/folder/folder").isdir("/file.txt")
    success[7] = not Storage(f"{bucket_name}/folder/folder/file.txt").isdir()
    success[8] = Storage().isdir(f"{bucket_name}")
    success[9] = not Storage().isdir(f"{bucket_name}/folder/folder/file.txt")

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    partition_cols = ["a"]
    file_name = f"gs://{bucket_name}/file.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table, file_name, partition_cols=partition_cols, basename_template="{i}.parquet"
    )

    success[10] = Storage(file_name).isdir()
    success[11] = Storage(file_name).isdir("a=1")
    success[12] = not Storage(file_name).isdir("a=1/0.parquet")

    failed = [k for k, v in success.items() if not v]

    Storage(bucket_name).delete()
    assert not failed


def test_is_file():
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    success = {}
    bucket_name = f"test_bucket_{uuid4()}"
    create_bucket(bucket_name)
    success[0] = not Storage(bucket_name).isfile()
    success[1] = not Storage(f"{bucket_name}/file").isfile()

    create_file(bucket_name, "folder/folder/file.txt")
    success[2] = not Storage(f"{bucket_name}/folder").isfile()
    success[3] = not Storage(f"{bucket_name}").isfile("folder")
    success[4] = not Storage(f"{bucket_name}/folder").isfile("folder")
    success[5] = not Storage(f"{bucket_name}/").isfile("folder/folder")
    success[6] = Storage(f"{bucket_name}/folder/folder").isfile("/file.txt")
    success[7] = Storage(f"{bucket_name}/folder/folder/file.txt").isfile()
    success[8] = Storage().isfile(f"{bucket_name}/folder/folder/file.txt")

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    partition_cols = ["a"]
    file_name = f"gs://{bucket_name}/file.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table, file_name, partition_cols=partition_cols, basename_template="{i}.parquet"
    )

    success[10] = not Storage(file_name).isfile()
    success[11] = not Storage(file_name).isfile("a=1")
    success[12] = Storage(file_name).isfile("a=1/0.parquet")

    Storage(bucket_name).delete()

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_write():
    success = {}
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"test_file_{uuid4()}.csv"
    file_path = f"{bucket_name}/{file_name}"
    create_bucket(bucket_name)
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    data_str = "\n".join([",".join(map(str, row)) for row in zip(*data.values())])

    # Test write w

    Storage(file_path).write(data_str)

    success[0] = Storage(file_path).exists()
    success[1] = Storage(file_path).isfile()

    with Storage(file_path).open() as f:
        read_file = f.read()

    success[2] = read_file == data_str

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_read():
    success = {}
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"test_file_{uuid4()}.csv"
    file_path = f"{bucket_name}/{file_name}"
    create_bucket(bucket_name)
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1, 2, 3],
    }
    data_str = "\n".join([",".join(map(str, row)) for row in zip(*data.values())])
    with Storage(file_path).open(mode="w") as f:
        f.write(data_str)
    success[0] = Storage(file_path).exists()
    success[1] = Storage(file_path).isfile()

    # Test read
    read_data = Storage(file_path).read()
    success[2] = read_data == data_str

    failed = [k for k, v in success.items() if not v]

    assert not failed
