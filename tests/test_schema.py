import pytest
import datetime
import pandas as pd
import pyarrow as pa
from google.cloud import bigquery

from gcp_tools.schema import (
    enforce_schema,
    infer_schema,
    Schema,
    compute_type_matches,
    get_equivalent_schema_dict,
    get_matching_schema_type,
    dict_to_pyarrow_fields,
)


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


def test_infer_schema():
    success = {}
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.datetime.now() for _ in range(3)],
    }
    python_schema = infer_schema(data)
    success[0] = python_schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
    }
    data = {"a": 1, "b": [2.0], "c": {"c1": ["w", "w"], "c2": [0, 1]}}
    python_schema = infer_schema(data)
    success[1] = python_schema == {
        "a": int,
        "b": float,
        "c": {"c1": str, "c2": int},
    }
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [1.0, 2.0, 3.0],
            "date": [datetime.datetime.now() for _ in range(3)],
        }
    )
    pandas_schema = infer_schema(data)
    success[2] = pandas_schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
    }
    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_schema():
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.datetime.now() for _ in range(3)],
    }
    inferred_schema = Schema(data, is_data=True).infer_schema()
    python_schema = inferred_schema.python()
    bigquery_schema = inferred_schema.bigquery()
    str_schema = inferred_schema.str()
    pyarrow_schema = inferred_schema.pyarrow()
    assert inferred_schema.schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
    }
    assert python_schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
    }
    assert bigquery_schema == [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("date", "DATETIME"),
    ]
    assert str_schema == {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
    }
    assert pyarrow_schema == pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("date", pa.timestamp("ns")),
        ]
    )


def test_convert_all_schemas():
    success = {}

    ### python ###
    python_schema = {"a": int, "b": str, "c": float}

    # 1. python -> pyarrow
    schema = Schema(python_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
        ]
    )
    success[0] = schema == pyarrow_schema

    # 2. python -> str
    schema = Schema(python_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float"}
    success[1] = schema == str_schema

    # 3. python -> bigquery
    schema = Schema(python_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
    ]
    success[2] = schema == bigquery_schema

    # 4. python -> pandas
    schema = Schema(python_schema).pandas()
    pandas_schema = {"a": "int64", "b": "object", "c": "float64"}
    success[3] = schema == pandas_schema

    ### pyarrow ###
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
        ]
    )

    # 1. pyarrow -> str
    schema = Schema(pyarrow_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float"}
    success[4] = schema == str_schema

    # 2. pyarrow -> python
    schema = Schema(pyarrow_schema).python()
    python_schema = {"a": int, "b": str, "c": float}
    success[5] = schema == python_schema

    # 3. pyarrow -> bigquery
    schema = Schema(pyarrow_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
    ]
    success[6] = schema == bigquery_schema

    # 4. pyarrow -> pandas
    schema = Schema(pyarrow_schema).pandas()
    pandas_schema = {"a": "int64", "b": "object", "c": "float64"}
    success[7] = schema == pandas_schema

    ### strings ###
    str_schema = {"a": "int", "b": "str", "c": "float"}

    # 1. str -> pyarrow
    schema = Schema(str_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
        ]
    )
    success[8] = schema == pyarrow_schema

    # 2. str -> python
    schema = Schema(str_schema).python()
    python_schema = {"a": int, "b": str, "c": float}
    success[9] = schema == python_schema

    # 3. str -> bigquery
    schema = Schema(str_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
    ]
    success[10] = schema == bigquery_schema

    # 4. str -> pandas
    schema = Schema(str_schema).pandas()
    pandas_schema = {"a": "int64", "b": "object", "c": "float64"}
    success[11] = schema == pandas_schema

    ### bigquery ###
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
    ]

    # 1. bigquery -> pyarrow
    schema = Schema(bigquery_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
        ]
    )
    success[12] = schema == pyarrow_schema

    # 2. bigquery -> str
    schema = Schema(bigquery_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float"}
    success[13] = schema == str_schema

    # 3. bigquery -> python
    schema = Schema(bigquery_schema).python()
    python_schema = {"a": int, "b": str, "c": float}
    success[14] = schema == python_schema

    # 4. bigquery -> pandas
    schema = Schema(bigquery_schema).pandas()
    pandas_schema = {"a": "int64", "b": "object", "c": "float64"}
    success[15] = schema == pandas_schema

    ### bigquery as a dict ###
    bigquery_schema = {
        "a": "INTEGER",
        "b": "STRING",
        "c": "FLOAT",
    }

    # 1. bigquery -> pyarrow
    schema = Schema(bigquery_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
        ]
    )
    success[16] = schema == pyarrow_schema

    # 2. bigquery -> str
    schema = Schema(bigquery_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float"}
    success[17] = schema == str_schema

    # 3. bigquery -> python
    schema = Schema(bigquery_schema).python()
    python_schema = {"a": int, "b": str, "c": float}
    success[18] = schema == python_schema

    # 4. bigquery -> pandas
    schema = Schema(bigquery_schema).pandas()
    pandas_schema = {"a": "int64", "b": "object", "c": "float64"}
    success[19] = schema == pandas_schema

    ### pyarrow as a dict ###
    pyarrow_schema = {
        "a": pa.int64(),
        "b": pa.string(),
        "c": {"c1": pa.float64(), "c2": pa.int64()},
    }

    # 1. pyarrow -> str
    schema = Schema(pyarrow_schema).str()
    str_schema = {
        "a": "int",
        "b": "str",
        "c": {"c1": "float", "c2": "int"},
    }
    success[20] = schema == str_schema

    # 2. pyarrow -> python
    schema = Schema(pyarrow_schema).python()
    python_schema = {
        "a": int,
        "b": str,
        "c": {"c1": float, "c2": int},
    }
    success[21] = schema == python_schema

    # 3. pyarrow -> bigquery
    schema = Schema(pyarrow_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField(
            "c",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("c1", "FLOAT"),
                bigquery.SchemaField("c2", "INTEGER"),
            ],
        ),
    ]
    success[22] = schema == bigquery_schema

    # 4. pyarrow -> pandas
    schema = Schema(pyarrow_schema).pandas()
    pandas_schema = {
        "a": "int64",
        "b": "object",
        "c": {"c1": "float64", "c2": "int64"},
    }
    success[23] = schema == pandas_schema

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_schema_from_dataframe():
    success = {}
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [1.0, 2.0, 3.0],
            "date": [datetime.datetime.now() for _ in range(3)],
        }
    )

    # string
    schema = Schema(df).str()
    str_schema = {"a": "int", "b": "str", "c": "float", "date": "datetime"}
    success[0] = schema == str_schema

    # python
    schema = Schema(df).python()
    python_schema = {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
    }
    success[1] = schema == python_schema

    # bigquery
    schema = Schema(df).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("date", "DATETIME"),
    ]
    success[2] = schema == bigquery_schema

    # pyarrow
    schema = Schema(df).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("date", pa.timestamp("ns")),
        ]
    )
    success[3] = schema == pyarrow_schema

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_schema_nested():
    success = {}
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.datetime.now() for _ in range(3)],
        "nested": {"a": [1, 2, 3], "b": ["a", "b", "c"]},
    }
    inferred_schema = Schema(data, is_data=True).infer_schema()
    python_schema = inferred_schema.python()
    bigquery_schema = inferred_schema.bigquery()
    str_schema = inferred_schema.str()
    pyarrow_schema = inferred_schema.pyarrow()
    success[0] = inferred_schema.schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
        "nested": {"a": int, "b": str},
    }
    success[1] = python_schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime.datetime,
        "nested": {"a": int, "b": str},
    }
    success[2] = bigquery_schema == [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("date", "DATETIME"),
        bigquery.SchemaField(
            "nested",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("a", "INTEGER"),
                bigquery.SchemaField("b", "STRING"),
            ],
        ),
    ]
    success[3] = str_schema == {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
        "nested": {"a": "int", "b": "str"},
    }
    success[4] = pyarrow_schema == pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("date", pa.timestamp("ns")),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field("a", pa.int64()),
                        pa.field("b", pa.string()),
                    ]
                ),
            ),
        ]
    )

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_dict_to_pyarrow_fields():
    success = {}

    schema = {"a": "int64", "b": "string", "c": "float64"}
    pa_fields = dict_to_pyarrow_fields(schema)
    exp_fields = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
        ]
    )
    success[0] = pa_fields == exp_fields

    schema = {
        "a": "int64",
        "b": "string",
        "c": "float64",
        "date": "timestamp[ns]",
        "nested": {"a": "int64", "b": "string"},
    }
    pa_fields = dict_to_pyarrow_fields(schema)
    exp_fields = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("date", pa.timestamp("ns")),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field("a", pa.int64()),
                        pa.field("b", pa.string()),
                    ]
                ),
            ),
        ]
    )
    success[1] = pa_fields == exp_fields

    schema = {
        "a": "int64",
        "b": ["string"],
        "nested": {"a": "int64", "b": ["string"]},
    }
    pa_fields = dict_to_pyarrow_fields(schema)
    exp_fields = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.list_(pa.string())),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field("a", pa.int64()),
                        pa.field("b", pa.list_(pa.string())),
                    ]
                ),
            ),
        ]
    )
    success[2] = pa_fields == exp_fields

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_compute_type_matches():
    success = {}

    schema = ["str", "float", "int"]
    target_schema = get_equivalent_schema_dict("bigquery").values()
    matches = compute_type_matches(schema, target_schema)
    success[0] = matches == 0
    target_schema = get_equivalent_schema_dict("python").values()
    matches = compute_type_matches(schema, target_schema)
    success[1] = matches == 0
    target_schema = get_equivalent_schema_dict("str").values()
    matches = compute_type_matches(schema, target_schema)
    success[2] = matches == 3

    schema = [str, float, int]
    target_schema = get_equivalent_schema_dict("bigquery").values()
    matches = compute_type_matches(schema, target_schema)
    success[3] = matches == 0
    target_schema = get_equivalent_schema_dict("python").values()
    matches = compute_type_matches(schema, target_schema)
    success[4] = matches == 3
    target_schema = get_equivalent_schema_dict("str").values()
    matches = compute_type_matches(schema, target_schema)
    success[5] = matches == 0

    schema = ["int64", "object", "float64"]
    target_schema = get_equivalent_schema_dict("bigquery").values()
    matches = compute_type_matches(schema, target_schema)
    success[6] = matches == 0
    target_schema = get_equivalent_schema_dict("python").values()
    matches = compute_type_matches(schema, target_schema)
    success[7] = matches == 0
    target_schema = get_equivalent_schema_dict("str").values()
    matches = compute_type_matches(schema, target_schema)
    success[8] = matches == 0
    target_schema = get_equivalent_schema_dict("pandas").values()
    matches = compute_type_matches(schema, target_schema)
    success[9] = matches == 3

    schema = ["int", "int64"]
    target_schema = get_equivalent_schema_dict("pandas").values()
    matches = compute_type_matches(schema, target_schema)
    success[10] = matches == 1
    target_schema = get_equivalent_schema_dict("str").values()
    matches = compute_type_matches(schema, target_schema)
    success[11] = matches == 1

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_get_matching_schema_type():
    success = {}

    schema = {"a": int, "b": str, "c": float}
    matching_type = get_matching_schema_type(schema)
    success[0] = matching_type == "python"

    schema = {"a": "int", "b": "str", "c": "float"}
    matching_type = get_matching_schema_type(schema)
    success[1] = matching_type == "str"

    schema = {"a": "INTEGER", "b": "STRING", "c": "FLOAT"}
    matching_type = get_matching_schema_type(schema)
    success[2] = matching_type == "bigquery"

    schema = {"a": "int64", "b": "object", "c": "float64"}
    matching_type = get_matching_schema_type(schema)
    success[3] = matching_type == "pandas"

    schema = {"a": "int", "b": "int64"}
    matching_type = get_matching_schema_type(schema)
    success[4] = matching_type is None

    schema = {"a": "int", "b": "ints"}
    matching_type = get_matching_schema_type(schema)
    success[5] = matching_type is None

    failed = [k for k, v in success.items() if not v]

    assert not failed
