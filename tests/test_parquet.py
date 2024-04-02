import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from uuid import uuid4

from gcp_tools import Parquet, Storage


def test_write_single():
    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"gs://{bucket_name}/file.parquet"
    Storage().create_bucket(bucket_name)
    Parquet(file_name).write(data)

    success[0] = Storage(file_name).exists()

    with Storage(file_name).open("rb") as f:
        table = pq.read_table(f)

    read_df = table.to_pandas()

    success[1] = data.equals(read_df)

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_write_partitioned():
    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"gs://{bucket_name}/file.parquet"
    Storage().create_bucket(bucket_name)
    Parquet(file_name).write(data, partition_cols=["a"])

    success[0] = Storage(file_name).exists()
    success[1] = Storage(f"{file_name}/a=1").exists()
    success[3] = Storage(f"{file_name}/a=3").exists()
    success[4] = Storage(f"{file_name}/a=1/0.parquet").exists()
    success[5] = Storage(f"{file_name}/a=3/0.parquet").exists()

    with Storage(f"{file_name}/a=1/0.parquet").open("rb") as f:
        table = pq.read_table(f)

    read_df = table.to_pandas()
    expected_df = pd.DataFrame({"b": [4]})

    success[6] = expected_df.equals(read_df)

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_read_single():
    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"gs://{bucket_name}/file.parquet"
    Storage().create_bucket(bucket_name)
    success[0] = not Storage(bucket_name).exists(file_name)
    Parquet(file_name).write(data)
    success[1] = Storage(bucket_name).exists(file_name)

    read_df = Parquet(file_name).read()

    success[1] = data.equals(read_df)

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_read_partitioned():
    from gcp_tools.schema import Schema

    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    schema = Schema(data).pyarrow()
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"gs://{bucket_name}/file.parquet"
    Storage().create_bucket(bucket_name)
    Parquet(file_name).write(data, partition_cols=["a"])

    success[0] = Storage().exists(file_name)

    read_df = Parquet(file_name).read(schema=schema)

    success[1] = set(data.columns) == set(read_df.columns)

    # Reorder columns
    read_df = read_df[data.columns]

    success[2] = data.equals(read_df)

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_read_partitions_only():
    from gcp_tools.schema import Schema

    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    schema = Schema(data).pyarrow()
    bucket_name = f"test_bucket_{uuid4()}"
    file_name = f"gs://{bucket_name}/file.parquet"
    Storage().create_bucket(bucket_name)
    Parquet(file_name).write(data, partition_cols=["a", "b"])

    read_df = Parquet(file_name).read(read_partitions_only=True, schema=schema)

    success[0] = set(data.columns) == set(read_df.columns)
    success[1] = data.convert_dtypes().equals(read_df)

    failed = [k for k, v in success.items() if not v]

    assert not failed
