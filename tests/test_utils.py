import pytest
import pandas as pd
from gcp_tools.utils import enforce_schema


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
