from datetime import datetime, date, time
from gcp_tools.utils import (
    force_list,
    is_series,
    is_dataframe,
    log,
    reverse_dict,
    is_pyarrow_schema,
    is_bigquery_schema,
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
        }
    elif target == "bigquery":
        return {
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
            "int": "int64",
            "float": "float64",
            "str": "object",
            "bool": "bool",
            "timestamp": "datetime64[ns]",
            "date": "datetime64[ns]",
            "time": "datetime64[ns]",
            "datetime": "datetime64[ns]",
            "bytes": "object",
            "array": "object",
            "struct": "object",
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
    if sum(matching_values) == 0:
        log("No matching schema type found.")
        return None
    elif sum([x == len(schema) for x in matching_values]) > 1:
        log("Multiple matching schema types found.")
        return None
    matching_type = max(type_matches, key=type_matches.get)
    if type_matches[matching_type] != len(schema):
        log("Not all schema types matched.")
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


def dtype_str_to_type(dtype_str):
    """
    Map a string representation of a dtype to its corresponding Python type
    or return the string if it's a Pandas-specific type.

    Args:
    - dtype_str (str): The string representation of the dtype.

    Returns:
    - type: The Python type corresponding to the dtype string.
    """
    python_types = {
        "int": int,
        "int64": int,
        "float": float,
        "float64": float,
        "str": str,
        "bool": bool,
        "object": str,
        "datetime64[ns]": datetime,
    }
    return python_types.get(dtype_str, dtype_str)


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


def bigquery_fields_to_dict(schema_fields):
    """
    Convert BigQuery schema fields to a dictionary.

    Args:
    - schema_fields (list[bigquery.SchemaField]): The BigQuery schema fields.

    Returns:
    - dict: The dictionary representation of the schema.
    """
    schema_dict = {}
    for field in schema_fields:
        schema_dict[field.name] = field.field_type
    return schema_dict


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


def pyarrow_fields_to_dict(schema_fields):
    """
    Convert PyArrow schema fields to a dictionary.

    Args:
    - schema_fields (pyarrow.Schema): The PyArrow schema.

    Returns:
    - dict: The dictionary representation of the schema.
    """
    schema_dict = {}
    # custom_field_map = {
    #     "double": "float64",
    # }
    for field in schema_fields:
        field_type = str(field.type)
        schema_dict[field.name] = field_type
    return schema_dict


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
        schema = dtype_str_to_type(schema)
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
            log(f"Error enforcing schema {col_schema} on {col}: {e}")
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
        for col, dtype in data.dtypes.items():
            schema[col] = dtype_str_to_type(str(dtype))
    elif isinstance(data, dict):
        for col, values in data.items():
            if isinstance(values, dict):
                schema[col] = infer_schema(values)
            elif values:
                value = values[0] if isinstance(values, list) else values
                if isinstance(value, int):
                    schema[col] = int
                elif isinstance(value, float):
                    schema[col] = float
                elif isinstance(value, str):
                    schema[col] = str
                elif isinstance(value, bool):
                    schema[col] = bool
                else:
                    schema[col] = type(value)
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
        self.input_schema = input
        self.schema = input
        self.is_data = is_data
        self.schema_type = schema_type or self.infer_schema_type()

        # Now the goal is to convert whatever schema into a dictionary of Python types
        if is_dataframe(input) or self.is_data:
            self.infer_schema()
        if self.schema_type == "bigquery":
            self.schema = bigquery_fields_to_dict(input)
        elif self.schema_type == "pyarrow":
            self.schema = pyarrow_fields_to_dict(input)

        if self.schema_type != "python":
            self.convert_schema_to_python()
        # Now the schema is a dictionary of Python types

    def __repr__(self):
        return f"Schema({self.schema})"

    def convert_schema_to_python(self):
        """
        Convert the schema to a Python dictionary.
        """
        self.schema = get_equivalent_schema(self.schema, self.schema_type, "python")
        self.schema_type = "python"

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
        if is_pyarrow_schema(self.input_schema):
            return "pyarrow"
        if is_bigquery_schema(self.input_schema):
            return "bigquery"
        if is_dataframe(self.input_schema) or self.is_data:
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
        if self.schema_type == "python":
            return self
        self.schema = infer_schema(self.input_schema, self.schema_type)
        self.schema_type = "python"
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

    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [1.0, 2.0, 3.0],
        "date": [datetime.now() for _ in range(3)],
    }
    inferred_schema = Schema(data, is_data=True)
    python_schema = inferred_schema.python()
    bigquery_schema = inferred_schema.bigquery()
    str_schema = inferred_schema.str()
    pyarrow_schema = inferred_schema.pyarrow()
    print(bigquery_schema)
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
