from datetime import datetime, date, time
from gcp_pal.utils import (
    force_list,
    is_series,
    is_dataframe,
    log,
    reverse_dict,
    is_pyarrow_schema,
    is_bigquery_schema,
    is_python_schema,
    get_dict_items,
)


def get_equivalent_schema_dict(target):
    """
    Get the equivalent schema dictionary in the target system.

    Args:
    - target (str): The target system.

    Returns:
    - dict: The equivalent schema dictionary in the target system.
    """
    if target == "str":
        return {
            "int": "int",
            "float": "float",
            "str": "str",
            "bool": "bool",
            "timestamp": "timestamp",
            "date": "date",
            "time": "time",
            "datetime": "datetime",
            "bytes": "bytes",
            "array": "array",
            "struct": "struct",
            "null": "null",
        }
    elif target == "python":
        return {
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "timestamp": datetime,
            "date": date,
            "time": time,
            "datetime": datetime,
            "bytes": bytes,
            "array": list,
            "struct": dict,
            "null": type(None),
        }
    elif target == "bigquery":
        return {
            "null": "BOOLEAN",
            "int": "INTEGER",
            "float": "FLOAT",
            "str": "STRING",
            "bool": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
            "time": "TIME",
            "datetime": "DATETIME",
            "bytes": "BYTES",
            "array": "ARRAY",
            "struct": "STRUCT",
        }
    elif target == "pandas":
        return {
            "int": "Int64",
            "float": "Float64",
            "str": "string",
            "bool": "boolean",
            "timestamp": "datetime64[ns]",
            "date": "datetime64[ns]",
            "time": "datetime64[ns]",
            "datetime": "datetime64[ns]",
            "bytes": "bytes",
            "array": "list",
            "struct": "struct",
            "null": "object",
        }
    elif target == "pyarrow":
        return {
            "int": "int64",
            "float": "double",
            "str": "string",
            "bool": "bool",
            "timestamp": "timestamp[ns]",
            "date": "date32",
            "time": "time64[ns]",
            "datetime": "timestamp[ns]",
            "bytes": "binary",
            "array": "list",
            "struct": "struct",
            "null": "null",
        }
    else:
        raise ValueError(f"Unsupported target system: {target}")


ALL_SUPPORTED_SCHEMA_TYPES = ["bigquery", "str", "python", "pandas", "pyarrow"]


def compute_type_matches(schema: list, target_schema: list):
    """
    Compute the type matches between two schema lists.

    Args:
    - schema (list): The input schema values.
    - target_schema (list): The target schema values against which to compare.

    Returns:
    - int: The number of type matches.
    """
    type_matches = 0
    for value in schema:
        if value in target_schema:
            type_matches += 1
    return type_matches


def get_matching_schema_type(schema):
    """
    Get the matching schema type for the schema.

    Args:
    - schema (dict): The schema.

    Returns:
    - str: The matching schema type.
    """
    if not isinstance(schema, list):
        schema_values = get_dict_items(schema, "value")
    else:
        schema_values = schema
    type_matches = {}
    for target in ALL_SUPPORTED_SCHEMA_TYPES:
        target_schema = get_equivalent_schema_dict(target)
        target_values = get_dict_items(target_schema, "value")
        type_matches[target] = compute_type_matches(schema_values, target_values)

    matching_values = type_matches.values()
    matching_type = max(type_matches, key=type_matches.get)
    if sum(matching_values) == 0:
        log("Schema - No matching schema type found.")
        return None
    elif sum([x == len(schema) for x in matching_values]) > 1:
        log("Schema - Multiple matching schema types found.")
        return None
    matching_type = max(type_matches, key=type_matches.get)
    if type_matches[matching_type] != len(schema_values):
        log("Schema - Not all schema types matched.")
        return None
    return matching_type


def get_equivalent_schema_type(schema_type, target="bigquery", direction="forward"):
    """
    Get the equivalent schema type in the target system.

    Args:
    - schema_type (str): The schema type.
    - target (str): The target system.
    - direction (str): The direction of the conversion. This is used for converting between systems.

    Returns:
    - str: The equivalent schema type in the target system.
    """
    schema_dict = get_equivalent_schema_dict(target, direction)
    if direction == "reverse":
        schema_dict = reverse_dict(schema_dict)
    if schema_type in schema_dict:
        return schema_dict[schema_type]
    else:
        raise ValueError(f"Unsupported schema type: {schema_type}")


def get_equivalent_schema(
    schema, origin=None, target=None, schema_from=None, schema_to=None
):
    """
    Get the equivalent schema in the target system.

    Args:
    - schema (dict): The schema to convert.
    - origin (str): The origin system.
    - target (str): The target system.

    Returns:
    - dict: The equivalent schema in the target system.
    """
    if origin is None:
        origin = "str"
    # In a parallel universe where Python is a functional language...
    # origin -> target = (str -> origin) -> target = (origin <- str) -> target
    equivalent_schema = {}
    if schema_from is None:
        schema_from = reverse_dict(get_equivalent_schema_dict(origin))
    if schema_to is None:
        schema_to = get_equivalent_schema_dict(target)
    for col, col_type in schema.items():
        if isinstance(col_type, dict):
            type_to = get_equivalent_schema(
                col_type, origin, target, schema_from, schema_to
            )
        else:
            type_from = schema_from[col_type]
            type_to = schema_to[type_from]
        equivalent_schema[col] = type_to
    return equivalent_schema


def dtype_str_to_type(dtype_str, target="str"):
    """
    Map a string representation of a Pandas dtype to its corresponding Python or str type
    or return the string if it's a Pandas-specific type.

    Args:
    - dtype_str (str): The string representation of the dtype.

    Returns:
    - type: The Python type corresponding to the dtype string.
    """
    if target == "python":
        target_types = {
            "int": int,
            "int64": int,
            "float": float,
            "float64": float,
            "str": str,
            "bool": bool,
            "object": str,
            "datetime64[ns]": datetime,
        }
    elif target == "str":
        target_types = {
            "int": "int",
            "int64": "int",
            "Int64": "int",
            "float": "float",
            "float64": "float",
            "Float64": "float",
            "str": "str",
            "string": "str",
            "bool": "bool",
            "boolean": "bool",
            "object": "str",
            "datetime64[ns]": "datetime",
        }
    return target_types.get(dtype_str, dtype_str)


def dict_to_bigquery_fields(schema_dict):
    """
    Convert a dictionary to a BigQuery schema.

    Args:
    - schema_dict (dict): The dictionary to convert.

    Returns:
    - list[bigquery.SchemaField]: The BigQuery schema.
    """
    from google.cloud import bigquery

    schema = []
    for col, col_type in schema_dict.items():
        if isinstance(col_type, dict):
            schema.append(
                bigquery.SchemaField(
                    col, "RECORD", fields=dict_to_bigquery_fields(col_type)
                )
            )
        else:
            schema.append(bigquery.SchemaField(col, col_type))

    return schema


def bigquery_fields_to_dict(schema):
    """
    Convert a schema to a list of columns.

    Args:
    - schema: list, the schema to convert.

    Returns:
    - (dict): The columns schema of the form
              {column_name: column_type, column_name: {nested_column_name: nested_column_type}}
    """
    columns = {}
    for field in schema:
        name = field.name
        fields = field.fields
        if fields:
            fields = bigquery_fields_to_dict(fields)
        columns[name] = fields
        if not fields:
            columns[name] = field.field_type
    return columns


def bigquery_fields_dict_to_dict(schema_fields_dict):
    """
    Convert BigQuery schema fields dictionary to a string dictionary.

    Args:
    - schema_fields_dict (dict[bigquery.SchemaField]): The BigQuery schema fields dictionary.

    Returns:
    - dict: The dictionary representation of the schema.

    Examples:
    >>> bigquery_fields_dict_to_dict({"a": bigquery.SchemaField("a", "INTEGER"),
                                      "b": bigquery.SchemaField("b", "STRING")})
        {"a": "INTEGER", "b": "STRING"}
    """
    schema_dict = {}
    for col, col_type in schema_fields_dict.items():
        if isinstance(col_type, dict):
            schema_dict[col] = bigquery_fields_dict_to_dict(col_type)
        elif isinstance(col_type, str):
            schema_dict[col] = col_type
        else:
            schema_dict[col] = col_type.field_type
    return schema_dict


def bigquery_to_dict(schema):
    """
    Convert a BigQuery schema to a dictionary.

    Args:
    - schema (list[bigquery.SchemaField]): The BigQuery schema.

    Returns:
    - dict[str, str]: The dictionary representation of the schema with string types.
    """
    if isinstance(schema, dict):
        output = bigquery_fields_dict_to_dict(schema)
    else:
        output = bigquery_fields_to_dict(schema)
    return output


def dict_to_pyarrow_fields(schema_dict):
    """
    Convert a dictionary to a PyArrow schema.

    Args:
    - schema_dict (dict): The dictionary to convert.

    Returns:
    - pyarrow.Schema: The PyArrow schema.
    """
    import pyarrow as pa

    fields = []
    if not isinstance(schema_dict, dict):
        return getattr(pa, schema_dict)()
    for col, col_type in schema_dict.items():
        if isinstance(col_type, dict):
            fields.append(pa.field(col, pa.struct(dict_to_pyarrow_fields(col_type))))
        elif isinstance(col_type, list):
            fields.append(pa.field(col, pa.list_(dict_to_pyarrow_fields(col_type[0]))))
        else:
            fields.append(pa.field(col, col_type))
    schema = pa.schema(fields)
    return schema


def pyarrow_fields_dict_to_dict(schema_fields_dict):
    """
    Convert PyArrow schema fields dictionary to a string dictionary.

    Args:
    - schema_fields_dict (dict[pyarrow.SchemaField]): The PyArrow schema fields dictionary.

    Returns:
    - dict: The dictionary representation of the schema.

    Examples:
    >>> pyarrow_fields_dict_to_dict({"a": pa.int64(), "b": pa.string(), "c": {"d": pa.float64()}})
    {"a": "int64", "b": "string", "c": {"d": "double"}}
    """
    schema_dict = {}
    for col, col_type in schema_fields_dict.items():
        if isinstance(col_type, dict):
            schema_dict[col] = pyarrow_fields_dict_to_dict(col_type)
        else:
            schema_dict[col] = str(col_type)
    return schema_dict


def pyarrow_fields_to_dict(schema_fields):
    """
    Convert PyArrow schema fields to a dictionary.

    Args:
    - schema_fields (pyarrow.schema): The PyArrow schema.

    Returns:
    - dict: The dictionary representation of the schema.
    """
    schema_dict = {}
    for field in schema_fields:
        field_type = field.type
        # Check if nested
        if field_type.num_fields > 0:
            schema_dict[field.name] = pyarrow_fields_to_dict(field_type)
        else:
            schema_dict[field.name] = str(field_type)
    return schema_dict


def pyarrow_to_dict(schema):
    """
    Convert a PyArrow schema to a dictionary.

    Args:
    - schema (pyarrow.Schema): The PyArrow schema as a dictionary or pa.schema object.

    Returns:
    - dict[str, str]: The dictionary representation of the schema with string types.
    """
    if isinstance(schema, dict):
        output = pyarrow_fields_dict_to_dict(schema)
    else:
        output = pyarrow_fields_to_dict(schema)
    return output


def type_to_str(schema_dict):
    """
    Convert a dictionary to a string representation of the schema.

    Args:
    - schema_dict (dict): The dictionary to convert.

    Returns:
    - str: The string representation of the schema.
    """
    schema_str = {}
    if schema_dict == []:
        return []
    if not isinstance(schema_dict, dict):
        return schema_dict.__name__
    for col, col_type in schema_dict.items():
        if isinstance(col_type, dict):
            schema_str[col] = {str(k): type_to_str(v) for k, v in col_type.items()}
        elif isinstance(col_type, list):
            schema_str[col] = [type_to_str(v) for v in col_type]
        else:
            schema_str[col] = col_type.__name__
    return schema_str


def ensure_types(df, types_dict):
    """
    Ensure the types of the columns of the df.
    """
    for c, c_type in types_dict.items():
        if c in df.columns:
            df[c] = df[c].astype(c_type)
    return df


def enforce_schema_on_series(series, schema):
    """
    Enforce a schema on a pandas Series.
    """
    if isinstance(schema, (type, str)):
        return series.astype(schema)
    elif isinstance(schema, type(lambda x: x)):
        return series.apply(schema)
    elif callable(schema):
        return schema(series)
    elif isinstance(schema, dict):
        return series.map(lambda x: schema.get(x, x))
    else:
        raise TypeError("Unsupported schema type.")


def enforce_schema_on_list(lst, schema):
    """
    Enforce a schema on a list.
    """
    if callable(schema):
        return [schema(x) for x in lst]
    elif isinstance(schema, dict):
        return [schema.get(x, x) for x in lst]
    elif isinstance(schema, type):
        return [schema(x) for x in lst]
    elif isinstance(schema, str):
        schema = dtype_str_to_type(schema, target="python")
        return [schema(x) for x in lst]
    else:
        raise TypeError("Unsupported schema type.")


def enforce_one_schema(data, col_schema):
    """
    Enforce a schema on a dataframe column or a list of data.

    Args:
    - data (Series|List): The data to enforce the schema on.
    - col_schema (type|dict|callable|list): The schema to enforce.

    Returns:
    - Series|List: The data with the schema enforced.
    """

    # Helper function to ensure data is a list
    def force_list(data):
        return data if isinstance(data, list) else [data]

    if isinstance(col_schema, list):
        # Attempt to enforce each schema in the list until one succeeds
        for schema in col_schema:
            try:
                return enforce_one_schema(data, schema)
            except Exception:
                continue
        else:
            raise ValueError(f"Could not enforce schema {col_schema} on {data}")
    else:
        if is_series(data):
            return enforce_schema_on_series(data, col_schema)
        else:
            data = force_list(data)
            return enforce_schema_on_list(data, col_schema)


def enforce_schema(df, schema={}, dtypes={}, errors="raise"):
    """
    Enforce a schema on a dataframe or dictionary.

    Args:
    - df (DataFrame|dict): The data to enforce the schema on.
    - schema (dict): The schema to enforce.
    - dtypes (dict): The object types to enforce. The schema will override these.
    - errors (str): How to handle errors.

    Returns:
    - DataFrame|dict: The data with the schema enforced.
    """
    schema = {**dtypes, **schema}  # schema takes precedence over dtypes
    if schema == {}:
        return df
    for col, col_schema in schema.items():
        if col not in df:
            df[col] = None
        try:
            df[col] = enforce_one_schema(df[col], col_schema)
        except Exception as e:
            log(f"Schema - Error enforcing schema {col_schema} on {col}: {e}")
            if errors == "raise":
                raise e
    return df


def infer_schema(data, schema_type="python"):
    """
    Infer a schema from a dataframe or dictionary.

    Args:
    - data (DataFrame|dict): The data to infer the schema from.

    Returns:
    - dict: The inferred schema.
    """
    schema = {}
    if is_dataframe(data):
        data = data.convert_dtypes(convert_integer=False)
        for col, dtype in data.dtypes.items():
            schema[col] = dtype_str_to_type(str(dtype))
    elif isinstance(data, dict):
        for col, values in data.items():
            if isinstance(values, dict):
                schema[col] = infer_schema(values)
            elif values or isinstance(values, bool) or values is None:
                non_null_values = [v for v in force_list(values) if v is not None]
                value = non_null_values[0] if non_null_values else None
                if value is None:
                    schema[col] = "null"
                elif isinstance(value, bool):
                    schema[col] = "bool"
                elif isinstance(value, int):
                    schema[col] = "int"
                elif isinstance(value, float):
                    schema[col] = "float"
                elif isinstance(value, str):
                    schema[col] = "str"
                else:
                    schema[col] = type(value).__name__
    return schema


class Schema:
    """
    A class for for bridging the gap between different schema representations of similar data.

    Args:
    - schema (dict): The input schema dictionary.
    - schema_type (str): The schema type. Options: "str", "python", "pandas", "pyarrow".

    Returns:
    - Schema: The schema object.

    Examples:
    - `Schema({"a": "int", "b": "str"}, schema_type="str").bigquery()` -> BigQuery schema
    - `Schema({"a": "int", "b": "str"}, schema_type="str").python()` -> Python schema
    - `Schema({"a": "int", "b": "str"}, schema_type="str").pandas()` -> Pandas schema
    - `Schema({"a": "int", "b": "str"}, schema_type="str").pyarrow()` -> PyArrow schema
    - `Schema({"a": "int", "b": "str"}, schema_type="str").str()` -> String schema
    """

    def __init__(self, input: dict = {}, schema_type: str = None, is_data=False):
        self.is_data = is_data if not is_dataframe(input) else True
        if self.is_data and isinstance(input, list) and isinstance(input[0], dict):
            input = input[0]
        self.input_schema = input
        self.schema = input
        self.schema_type = schema_type or self.infer_schema_type()

        # Now the goal is to convert whatever schema into a dictionary of Python types
        if is_dataframe(input) or self.is_data:
            self.infer_schema()
        if self.schema_type == "bigquery":
            self.schema = bigquery_to_dict(input)
        elif self.schema_type == "pyarrow":
            self.schema = pyarrow_to_dict(input)
        if self.schema_type != "str":
            self.convert_schema_to_str()
        # Now the schema is a dictionary of Python types

    def __repr__(self):
        return f"Schema({self.schema})"

    def convert_schema_to_str(self):
        """
        Convert the schema to a Python dictionary.
        """
        self.schema = get_equivalent_schema(self.schema, self.schema_type, "str")
        self.schema_type = "str"

    def infer_schema_type(self):
        """
        Infer the schema type from the input.

        Returns:
        - str: The schema type.

        Examples:
        - `Schema({"a": int, "b": str}).infer_schema_type()` -> "python"
        - `Schema({"a": "int", "b": "str"}).infer_schema_type()` -> "str"
        - `Schema({"a": "int64", "b": "str"}).infer_schema_type()` -> "pandas"
        - `Schema(pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])).infer_schema_type()` -> "pyarrow"
        - `Schema([bigquery.SchemaField("a", "INTEGER"), bigquery.SchemaField("b", "STRING")]).infer_schema_type()` -> "bigquery"
        """
        if not self.is_data and is_pyarrow_schema(self.input_schema):
            return "pyarrow"
        if not self.is_data and is_bigquery_schema(self.input_schema):
            return "bigquery"
        if not self.is_data and is_python_schema(self.input_schema):
            return "python"
        if self.is_data or is_dataframe(self.input_schema):
            # We return None here because self.input_schema is not a schema dictionary
            return None
        if not isinstance(self.input_schema, dict):
            return None
        matching_type = get_matching_schema_type(self.input_schema)

        return matching_type

    def infer_schema(self) -> "Schema":
        """
        Infer the schema from the input.

        Returns:
        - Schema: The schema object with the schema set to the inferred schema.
        """
        if self.schema_type == "str":
            return self
        self.schema = infer_schema(self.input_schema, self.schema_type)
        self.schema_type = "str"
        return self

    def bigquery(self) -> dict:
        if self.schema_type == "bigquery":
            return self.schema
        output_schema = get_equivalent_schema(self.schema, self.schema_type, "bigquery")
        output_schema = dict_to_bigquery_fields(output_schema)
        return output_schema

    def python(self) -> dict:
        if self.schema_type == "python":
            return self.schema
        output_schema = get_equivalent_schema(self.schema, self.schema_type, "python")
        return output_schema

    def pandas(self) -> dict:
        if self.schema_type == "pandas":
            return self.schema
        output_schema = get_equivalent_schema(self.schema, self.schema_type, "pandas")
        return output_schema

    def pyarrow(self) -> dict:
        if self.schema_type == "pyarrow":
            return self.schema
        output_schema = get_equivalent_schema(self.schema, self.schema_type, "pyarrow")
        output_schema = dict_to_pyarrow_fields(output_schema)
        return output_schema

    def str(self) -> str:
        if self.schema_type == "str":
            return self.schema
        output_schema = type_to_str(self.schema)
        return output_schema


if __name__ == "__main__":
    import pandas as pd
    import pyarrow as pa

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

    print(schema)

    exit()

if __name__ == "__main__":
    import pandas as pd
    import pyarrow as pa

    pyarrow_schema = {"a": pa.int64(), "b": pa.string(), "c": {"d": pa.float64()}}

    # 1. pyarrow -> str
    schema = Schema(pyarrow_schema).str()
    str_schema = {"a": "int", "b": "str", "c": "float"}
    print(str_schema)
    exit()

    inferred_schema = Schema(df, schema_type="pandas").infer_schema()
    python_schema = inferred_schema.python()
    bigquery_schema = inferred_schema.bigquery()
    str_schema = inferred_schema.str()
    pyarrow_schema = inferred_schema.pyarrow()
    print("Inferred schema:", inferred_schema.schema)
    print("Python schema:", python_schema)
    print("BigQuery schema:", bigquery_schema)
    print("String schema:", str_schema)
    print("PyArrow schema:", pyarrow_schema)
    exit()
