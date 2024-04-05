import pandas as pd
from gcp_tools.utils import (
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
