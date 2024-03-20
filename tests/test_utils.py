import pandas as pd
from gcp_tools.utils import is_series, force_list, is_dataframe


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
