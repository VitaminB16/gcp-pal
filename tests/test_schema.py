import pytest
import pandas as pd
import pyarrow as pa
from datetime import datetime
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
        "date": [datetime.now() for _ in range(3)],
    }
    python_schema = infer_schema(data)
    success[0] = python_schema == {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
    }
    data = {"a": 1, "b": [2.0], "c": {"c1": ["w", "w"], "c2": [0, 1]}}
    python_schema = infer_schema(data)
    success[1] = python_schema == {
        "a": "int",
        "b": "float",
        "c": {"c1": "str", "c2": "int"},
    }
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [1.0, 2.0, 3.0],
            "date": [datetime.now() for _ in range(3)],
        }
    )
    pandas_schema = infer_schema(data)
    success[2] = pandas_schema == {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
    }
    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_schema():
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.now() for _ in range(3)],
    }
    inferred_schema = Schema(data, is_data=True).infer_schema()
    python_schema = inferred_schema.python()
    bigquery_schema = inferred_schema.bigquery()
    str_schema = inferred_schema.str()
    pyarrow_schema = inferred_schema.pyarrow()
    assert inferred_schema.schema == {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
    }
    assert python_schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime,
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
    python_schema = {"a": int, "b": str, "c": float, "d": datetime}

    # 1. python -> pyarrow
    schema = Schema(python_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("d", pa.timestamp("ns")),
        ]
    )
    success[0] = schema == pyarrow_schema

    # 2. python -> str
    schema = Schema(python_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float", "d": "datetime"}
    success[1] = schema == str_schema

    # 3. python -> bigquery
    schema = Schema(python_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("d", "DATETIME"),
    ]
    success[2] = schema == bigquery_schema

    # 4. python -> pandas
    schema = Schema(python_schema).pandas()
    pandas_schema = {"a": "Int64", "b": "string", "c": "Float64", "d": "datetime64[ns]"}
    success[3] = schema == pandas_schema

    ### pyarrow ###
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("d", pa.timestamp("ns")),
        ]
    )

    # 1. pyarrow -> str
    schema = Schema(pyarrow_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float", "d": "datetime"}
    success[4] = schema == str_schema

    # 2. pyarrow -> python
    schema = Schema(pyarrow_schema).python()
    python_schema = {"a": int, "b": str, "c": float, "d": datetime}
    success[5] = schema == python_schema

    # 3. pyarrow -> bigquery
    schema = Schema(pyarrow_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("d", "DATETIME"),
    ]
    success[6] = schema == bigquery_schema

    # 4. pyarrow -> pandas
    schema = Schema(pyarrow_schema).pandas()
    pandas_schema = {"a": "Int64", "b": "string", "c": "Float64", "d": "datetime64[ns]"}
    success[7] = schema == pandas_schema

    ### strings ###
    str_schema = {"a": "int", "b": "str", "c": "float", "d": "timestamp"}

    # 1. str -> pyarrow
    schema = Schema(str_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("d", pa.timestamp("ns")),
        ]
    )
    success[8] = schema == pyarrow_schema

    # 2. str -> python
    schema = Schema(str_schema).python()
    python_schema = {"a": int, "b": str, "c": float, "d": datetime}
    success[9] = schema == python_schema

    # 3. str -> bigquery
    schema = Schema(str_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("d", "TIMESTAMP"),
    ]
    success[10] = schema == bigquery_schema

    # 4. str -> pandas
    schema = Schema(str_schema).pandas()
    pandas_schema = {"a": "Int64", "b": "string", "c": "Float64", "d": "datetime64[ns]"}
    success[11] = schema == pandas_schema

    ### bigquery ###
    bigquery_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("d", "TIMESTAMP"),
    ]

    # 1. bigquery -> pyarrow
    schema = Schema(bigquery_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("d", pa.timestamp("ns")),
        ]
    )
    success[12] = schema == pyarrow_schema

    # 2. bigquery -> str
    schema = Schema(bigquery_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float", "d": "timestamp"}
    success[13] = schema == str_schema

    # 3. bigquery -> python
    schema = Schema(bigquery_schema).python()
    python_schema = {"a": int, "b": str, "c": float, "d": datetime}
    success[14] = schema == python_schema

    # 4. bigquery -> pandas
    schema = Schema(bigquery_schema).pandas()
    pandas_schema = {"a": "Int64", "b": "string", "c": "Float64", "d": "datetime64[ns]"}
    success[15] = schema == pandas_schema

    ### bigquery as a dict ###
    bigquery_schema = {
        "a": "INTEGER",
        "b": "STRING",
        "c": "FLOAT",
        "d": "TIMESTAMP",
    }

    # 1. bigquery -> pyarrow
    schema = Schema(bigquery_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("d", pa.timestamp("ns")),
        ]
    )
    success[16] = schema == pyarrow_schema

    # 2. bigquery -> str
    schema = Schema(bigquery_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float", "d": "timestamp"}
    success[17] = schema == str_schema

    # 3. bigquery -> python
    schema = Schema(bigquery_schema).python()
    python_schema = {"a": int, "b": str, "c": float, "d": datetime}
    success[18] = schema == python_schema

    # 4. bigquery -> pandas
    schema = Schema(bigquery_schema).pandas()
    pandas_schema = {"a": "Int64", "b": "string", "c": "Float64", "d": "datetime64[ns]"}
    success[19] = schema == pandas_schema

    ### pyarrow as a dict ###
    pyarrow_schema = {
        "a": pa.int64(),
        "b": pa.string(),
        "c": {"c1": pa.float64(), "c2": pa.int64()},
        "d": pa.timestamp("ns"),
    }

    # 1. pyarrow -> str
    schema = Schema(pyarrow_schema).str()
    str_schema = {
        "a": "int",
        "b": "str",
        "c": {"c1": "float", "c2": "int"},
        "d": "datetime",
    }
    success[20] = schema == str_schema

    # 2. pyarrow -> python
    schema = Schema(pyarrow_schema).python()
    python_schema = {
        "a": int,
        "b": str,
        "c": {"c1": float, "c2": int},
        "d": datetime,
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
        bigquery.SchemaField("d", "DATETIME"),
    ]
    success[22] = schema == bigquery_schema

    # 4. pyarrow -> pandas
    schema = Schema(pyarrow_schema).pandas()
    pandas_schema = {
        "a": "Int64",
        "b": "string",
        "c": {"c1": "Float64", "c2": "Int64"},
        "d": "datetime64[ns]",
    }
    success[23] = schema == pandas_schema

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_convert_nested_schema():

    success = {}
    str_schema = {
        "name": "str",
        "age": "int",
        "income": "float",
        "is_student": "bool",
        "created_at": {
            "date": "datetime",
            "time": "timestamp",
        },
        "details": {
            "address": "str",
            "phone": "int",
        },
    }

    # 1. str -> pyarrow
    schema = Schema(str_schema).pyarrow()
    pyarrow_schema = pa.schema(
        [
            pa.field("name", pa.string()),
            pa.field("age", pa.int64()),
            pa.field("income", pa.float64()),
            pa.field("is_student", pa.bool_()),
            pa.field(
                "created_at",
                pa.struct(
                    [
                        pa.field("date", pa.timestamp("ns")),
                        pa.field("time", pa.timestamp("ns")),
                    ]
                ),
            ),
            pa.field(
                "details",
                pa.struct(
                    [
                        pa.field("address", pa.string()),
                        pa.field("phone", pa.int64()),
                    ]
                ),
            ),
        ]
    )
    success[0] = schema == pyarrow_schema

    # 2. str -> python
    schema = Schema(str_schema).python()
    python_schema = {
        "name": str,
        "age": int,
        "income": float,
        "is_student": bool,
        "created_at": {
            "date": datetime,
            "time": datetime,
        },
        "details": {
            "address": str,
            "phone": int,
        },
    }
    success[1] = schema == python_schema

    # 3. str -> bigquery
    schema = Schema(str_schema).bigquery()
    bigquery_schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("income", "FLOAT"),
        bigquery.SchemaField("is_student", "BOOLEAN"),
        bigquery.SchemaField(
            "created_at",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("date", "DATETIME"),
                bigquery.SchemaField("time", "TIMESTAMP"),
            ],
        ),
        bigquery.SchemaField(
            "details",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("phone", "INTEGER"),
            ],
        ),
    ]
    success[2] = schema == bigquery_schema

    # 4. str -> pandas
    schema = Schema(str_schema).pandas()
    pandas_schema = {
        "name": "string",
        "age": "Int64",
        "income": "Float64",
        "is_student": "boolean",
        "created_at": {"date": "datetime64[ns]", "time": "datetime64[ns]"},
        "details": {"address": "string", "phone": "Int64"},
    }
    success[3] = schema == pandas_schema

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_schema_from_dataframe():
    success = {}
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": [1.0, 2.0, 3.0],
            "date": [datetime.now() for _ in range(3)],
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
        "date": datetime,
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

    # pandas
    schema = Schema(df).pandas()
    pandas_schema = {
        "a": "Int64",
        "b": "string",
        "c": "Float64",
        "date": "datetime64[ns]",
    }
    success[4] = schema == pandas_schema

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_schema_nested():
    success = {}
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.now() for _ in range(3)],
        "nested": {"a": [1, 2, 3], "b": ["a", "b", "c"]},
    }
    inferred_schema = Schema(data, is_data=True).infer_schema()
    python_schema = inferred_schema.python()
    bigquery_schema = inferred_schema.bigquery()
    str_schema = inferred_schema.str()
    pyarrow_schema = inferred_schema.pyarrow()
    pandas_schema = inferred_schema.pandas()
    success[0] = inferred_schema.schema == {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
        "nested": {"a": "int", "b": "str"},
    }
    success[1] = python_schema == {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime,
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
    success[5] = pandas_schema == {
        "a": "Int64",
        "b": "string",
        "c": "Float64",
        "date": "datetime64[ns]",
        "nested": {"a": "Int64", "b": "string"},
    }

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


def test_infer_schema_nulls_from_data():
    success = {}

    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.now() for _ in range(3)],
        "nested": {"n1": True, "n2": False, "n3": None},
        "d": [3, None, 4],
    }
    schema = Schema(data, is_data=True)

    str_schema = schema.str()
    exp_schema = {
        "a": "int",
        "b": "str",
        "c": "float",
        "date": "datetime",
        "nested": {"n1": "bool", "n2": "bool", "n3": "null"},
        "d": "int",
    }
    success[0] = str_schema == exp_schema

    python_schema = schema.python()
    exp_schema = {
        "a": int,
        "b": str,
        "c": float,
        "date": datetime,
        "nested": {"n1": bool, "n2": bool, "n3": type(None)},
        "d": int,
    }
    success[1] = python_schema == exp_schema

    bigquery_schema = schema.bigquery()
    exp_schema = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "STRING"),
        bigquery.SchemaField("c", "FLOAT"),
        bigquery.SchemaField("date", "DATETIME"),
        bigquery.SchemaField(
            "nested",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("n1", "BOOLEAN"),
                bigquery.SchemaField("n2", "BOOLEAN"),
                bigquery.SchemaField("n3", "BOOLEAN"),
            ],
        ),
        bigquery.SchemaField("d", "INTEGER"),
    ]
    success[2] = bigquery_schema == exp_schema

    pyarrow_schema = schema.pyarrow()
    exp_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", pa.float64()),
            pa.field("date", pa.timestamp("ns")),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field("n1", pa.bool_()),
                        pa.field("n2", pa.bool_()),
                        pa.field("n3", pa.null()),
                    ]
                ),
            ),
            pa.field("d", pa.int64()),
        ]
    )
    success[3] = pyarrow_schema == exp_schema

    pandas_schema = schema.pandas()
    exp_schema = {
        "a": "Int64",
        "b": "string",
        "c": "Float64",
        "date": "datetime64[ns]",
        "nested": {"n1": "boolean", "n2": "boolean", "n3": "object"},
        "d": "Int64",
    }

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
    success[9] = matches == 1

    schema = ["int", "int64"]
    target_schema = get_equivalent_schema_dict("pandas").values()
    matches = compute_type_matches(schema, target_schema)
    success[10] = matches == 0
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

    schema = {"a": "Int64", "b": "string", "c": "Float64"}
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
