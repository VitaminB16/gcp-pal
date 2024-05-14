import pandas as pd
from gcp_pal.utils import (
    is_series,
    force_list,
    is_dataframe,
    is_pyarrow_schema,
    is_bigquery_schema,
    is_numpy_array,
    is_python_schema,
    reverse_dict,
    get_dict_items,
    orient_dict,
)


def test_is_series():
    success = {}
    success[0] = is_series(pd.Series([1, 2, 3])) == True
    success[1] = is_series([1, 2, 3]) == False
    success[2] = is_series(1) == False
    success[3] = is_series("a") == False
    success[4] = is_series(None) == False
    success[5] = is_series(True) == False
    success[6] = is_series(False) == False
    df = pd.DataFrame({"a": [1, 2, 3]})
    success[7] = is_series(df["a"]) == True

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_is_dataframe():
    success = {}
    df = pd.DataFrame({"a": [1, 2, 3]})
    success[0] = is_dataframe([1, 2, 3]) == False
    success[1] = is_dataframe(1) == False
    success[2] = is_dataframe("a") == False
    success[3] = is_dataframe(None) == False
    success[4] = is_dataframe(True) == False
    success[5] = is_dataframe(False) == False
    success[6] = is_dataframe(df["a"]) == False
    success[7] = is_dataframe(df) == True

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_force_list():
    success = {}
    success[0] = force_list([1, 2, 3]) == [1, 2, 3]
    success[1] = force_list(1) == [1]
    success[2] = force_list("a") == ["a"]
    success[3] = force_list(None) == [None]
    success[4] = force_list(True) == [True]
    success[5] = force_list(False) == [False]
    df = pd.DataFrame({"a": [1, 2, 3]})
    success[6] = force_list(df["a"]) == [df["a"]]
    success[7] = force_list(df) == [df]
    success[8] = force_list({"a": [1, 2, 3]}.keys()) == {"a": [1, 2, 3]}.keys()

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_reverse_dict():
    success = {}
    d = {"a": 1, "b": 2, "c": 3}
    success[0] = reverse_dict(d) == {1: "a", 2: "b", 3: "c"}
    d = {}
    success[1] = reverse_dict(d) == {}

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_get_dict_items():
    success = {}

    t = get_dict_items({"a": 1, "b": 2, "c": 3}, item_type="key")
    success[0] = set(t) == set(["a", "b", "c"])
    t = get_dict_items({"a": 1, "b": 2, "c": 3}, item_type="value")
    success[1] = set(t) == set([1, 2, 3])
    t = get_dict_items({"a": 1, "b": {"c": 2}, "d": 3}, item_type="value")
    success[2] = set(t) == set([1, 2, 3])
    t = get_dict_items({"a": 1, "b": {"c": 2}, "d": 3}, item_type="key")
    success[3] = set(t) == set(["a", "b", "c", "d"])
    t = get_dict_items({"a": 1, "b": {"c": 2}, "d": [3, 4]}, item_type="key")
    success[4] = set(t) == set(["a", "b", "c", "d"])
    t = get_dict_items({"a": 1, "b": {"c": 2}, "d": [3, {"e": 4}]}, item_type="key")
    success[5] = set(t) == set(["a", "b", "c", "d", "e"])
    t = get_dict_items({"a": 1, "b": {"c": 2}, "d": [3, {"e": 4}]}, item_type="value")
    success[6] = set(t) == set([1, 2, 3, 4])
    t = get_dict_items({"a": 1, "b": [1, [2, [3, 4]]]}, item_type="value")
    success[7] = set(t) == set([1, 1, 2, 3, 4])

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_is_pyarrow_schema():
    import pyarrow as pa

    success = {}

    schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("d", pa.list_(pa.int64())),
        ]
    )
    success[0] = is_pyarrow_schema(schema) == True

    schema = pa.schema([])

    success[1] = is_pyarrow_schema(schema) == False

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_is_numpy_array():
    df = pd.DataFrame({"a": [1, 2, 3]})

    success = {}

    success[0] = is_numpy_array(df["a"].values) == True
    success[1] = is_numpy_array(df.values) == True
    success[2] = is_numpy_array(df) == False
    success[3] = is_numpy_array([1, 2, 3]) == False
    success[4] = is_numpy_array(1) == False
    success[5] = is_numpy_array("a") == False
    success[6] = is_numpy_array(None) == False
    success[7] = is_numpy_array(True) == False
    success[8] = is_numpy_array(False) == False

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_is_python_schema():
    success = {}

    schema = {
        "a": int,
        "b": str,
        "c": float,
        "d": list,
    }
    success[0] = is_python_schema(schema) == True

    schema = {}
    success[1] = is_python_schema(schema) == False

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_is_bigquery_schema():
    success = {}

    from google.cloud.bigquery import SchemaField

    schema = [
        SchemaField("a", "INTEGER"),
        SchemaField("b", "STRING"),
        SchemaField("c", "FLOAT"),
        SchemaField("d", "RECORD"),
    ]
    success[0] = is_bigquery_schema(schema) == True

    schema = []
    success[1] = is_bigquery_schema(schema) == False

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_orient_dict():
    success = {}

    d = {"a": 1, "b": 2, "c": 3}
    t = orient_dict(d, "columns")
    success[0] = t == d
    t = orient_dict(d, "index")
    success[1] = t == [{"a": 1, "b": 2, "c": 3}]

    d = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    t = orient_dict(d, "columns")
    success[2] = t == d
    t = orient_dict(d, "index")
    t_index = [
        {"a": 1, "b": 4, "c": 7},
        {"a": 2, "b": 5, "c": 8},
        {"a": 3, "b": 6, "c": 9},
    ]
    success[3] = t == t_index

    d = [{"a": 1, "b": 4, "c": 7}, {"a": 2, "b": 5, "c": 8}, {"a": 3, "b": 6, "c": 9}]
    t = orient_dict(d, "columns")
    success[4] = t == {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    t = orient_dict(d, "index")
    success[5] = t == d

    d = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    tt = orient_dict(orient_dict(d, "index"), "columns")
    success[6] = tt == d

    # d = {}
    # t = orient_dict(d, "columns")
    # success[7] = t == d

    # d = [{}]
    # t = orient_dict(d, "columns")
    # success[8] = t == d

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_zip_directory():
    import os
    import shutil
    from zipfile import ZipFile
    from gcp_pal.utils import zip_directory

    success = {}
    dir_name = "test_dir_123"
    file_name = "test_dir_456/test_file.txt"
    file_path = f"{dir_name}/{file_name}"

    os.makedirs(dir_name, exist_ok=True)
    os.makedirs(f"{dir_name}/test_dir_456", exist_ok=True)
    with open(file_path, "w") as f:
        f.write("test")

    zip_name = zip_directory(dir_name)
    success[0] = zip_name == "test_dir_123.zip"
    try:
        success[1] = os.path.exists(zip_name)
    except Exception:
        success[1] = False

    try:
        with ZipFile(zip_name, "r") as z:
            z.extractall("test_dir_123_extracted")
        success[2] = os.path.exists(f"test_dir_123_extracted/{file_name}")
    except Exception:
        success[2] = False

    try:
        with open(f"test_dir_123_extracted/{file_name}", "r") as f:
            success[3] = f.read() == "test"
    except Exception:
        success[3] = False

    shutil.rmtree(dir_name)
    shutil.rmtree("test_dir_123_extracted")
    os.remove(zip_name)

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_module_handler():
    from gcp_pal.utils import ModuleHandler

    success = {}

    math = ModuleHandler("math").please_import()

    success[0] = math is not None

    success[1] = "sqrt" in dir(math)
    success[2] = math.sqrt(4) - 2.0 < 1e-6

    math2 = ModuleHandler("math").please_import()

    success[3] = math2 is math
    success[4] = math2 == math

    bq1 = ModuleHandler("google.cloud").please_import("bigquery")
    bq2 = ModuleHandler("google.cloud.bigquery").please_import()

    success[5] = bq1 is bq2
    success[6] = bq1 == bq2

    bq_client1 = bq1.Client()
    bq_client2 = ModuleHandler("google.cloud.bigquery").please_import("Client")()
    success[7] = sorted(dir(bq_client1)) == sorted(dir(bq_client2))

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_client_handler():
    from gcp_pal.utils import ClientHandler, ModuleHandler

    success = {}
    bigquery = ModuleHandler("google.cloud").please_import("bigquery")
    bq_client1 = ClientHandler(bigquery.Client).get()
    bq_client2 = ClientHandler(bigquery.Client).get()

    success[0] = bq_client1 is bq_client2
    success[1] = bq_client1 == bq_client2

    bq_client3 = ClientHandler(bigquery.Client).get(location="europe-west1")
    success[2] = bq_client3 is not bq_client1

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_client_handler():
    from gcp_pal.utils import ClientHandler
    from google.cloud import bigquery, firestore

    bq_client1 = ClientHandler(bigquery.Client).get()
    bq_client2 = ClientHandler(bigquery.Client).get()
    bq_client3 = ClientHandler(bigquery.Client).get(force_refresh=True)

    fs_client1 = ClientHandler(firestore.Client).get()
    fs_client2 = ClientHandler(firestore.Client).get(project="my-project")
    fs_client3 = ClientHandler(firestore.Client).get(force_refresh=True)
    fs_client4 = ClientHandler(firestore.Client).get(project="my-project")

    assert bq_client1 is bq_client2
    assert bq_client1 is not bq_client3

    assert fs_client1 is not fs_client2
    assert fs_client1 is not fs_client3
    assert fs_client2 is fs_client4

    assert len(ClientHandler._clients) == 3


def test_lazy_loader():
    from gcp_pal.utils import LazyLoader

    bq = LazyLoader("google.cloud.bigquery")
    bq_client = bq.Client()
    assert bq_client is not None
    assert bq_client.__class__.__name__ == "Client"
    assert bq_client.__module__ == "google.cloud.bigquery.client"
