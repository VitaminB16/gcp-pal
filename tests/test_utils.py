import pytest
import pandas as pd
from gcp_tools.utils import enforce_schema, is_series, force_list, is_dataframe


@pytest.mark.parametrize(
    "data",
    [
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [1, 2, 3],
        },
        pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": ["a", "b", "c"],
                "c": [1, 2, 3],
            }
        ),
    ],
)
def test_enforce_schema(data):
    schema = {
        "a": float,
        "b": lambda x: x.upper(),
        "c": {1: "one", 2: "two", 3: "three"},
    }
    d = enforce_schema(data, schema)
    output = d
    if isinstance(data, pd.DataFrame):
        output = d.to_dict(orient="list")
    assert output["a"] == [1.0, 2.0, 3.0]
    assert output["b"] == ["A", "B", "C"]
    assert output["c"] == ["one", "two", "three"]


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
