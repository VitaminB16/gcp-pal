import pandas as pd
from gcp_tools.utils import (
    is_series,
    force_list,
    is_dataframe,
    reverse_dict,
    get_dict_items,
)


def test_is_series():
    assert is_series(pd.Series([1, 2, 3])) == True
    assert is_series([1, 2, 3]) == False
    assert is_series(1) == False
    assert is_series("a") == False
    assert is_series(None) == False
    assert is_series(True) == False
    assert is_series(False) == False
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert is_series(df["a"]) == True


def test_is_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert is_dataframe([1, 2, 3]) == False
    assert is_dataframe(1) == False
    assert is_dataframe("a") == False
    assert is_dataframe(None) == False
    assert is_dataframe(True) == False
    assert is_dataframe(False) == False
    assert is_dataframe(df["a"]) == False
    assert is_dataframe(df) == True


def test_force_list():
    assert force_list([1, 2, 3]) == [1, 2, 3]
    assert force_list(1) == [1]
    assert force_list("a") == ["a"]
    assert force_list(None) == [None]
    assert force_list(True) == [True]
    assert force_list(False) == [False]
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert force_list(df["a"]) == [df["a"]]
    assert force_list(df) == [df]
    assert force_list({"a": [1, 2, 3]}.keys()) == {"a": [1, 2, 3]}.keys()


def test_reverse_dict():
    d = {"a": 1, "b": 2, "c": 3}
    assert reverse_dict(d) == {1: "a", 2: "b", 3: "c"}
    d = {}
    assert reverse_dict(d) == {}


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
