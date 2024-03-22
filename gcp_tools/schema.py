from gcp_tools.utils import force_list, is_series, is_dataframe, log, reverse_dict


def get_equivalent_schema_dict(target):
    """
    Get the equivalent schema dictionary in the target system.

    Args:
    - target (str): The target system.

    Returns:
    - dict: The equivalent schema dictionary in the target system.
    """
    if target == "bigquery":
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
    elif target == "str":
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
        from pandas import Timestamp

        return {
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "timestamp": Timestamp,
            "date": Timestamp,
            "time": Timestamp,
            "datetime": Timestamp,
            "bytes": bytes,
            "array": list,
            "struct": dict,
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
            "float": "float64",
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
        schema.append(bigquery.SchemaField(col, col_type))

    return schema


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
    Ensure the types of the columns of the df
    """
    for c, c_type in types_dict.items():
        if c in df.columns:
            df[c] = df[c].astype(c_type)
    return df


def enforce_schema_on_series(series, schema):
    """Enforce a schema on a pandas Series."""
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
    """Enforce a schema on a list."""
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


def infer_schema(data):
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
            if values:
                value = values[0]
                if isinstance(value, (int, float)):
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
    """

    def __init__(self, schema: dict = {}):
        self.input_schema = schema
        self.schema = schema
        self.schema_type = "dict"

    def bigquery(self) -> "Schema":
        """
        Convert a dictionary to a BigQuery schema.

        Args:
        - `schema_dict` (dict): The dictionary to convert.

        Returns:
        - `Schema`: The Schema object with BigQuery `schema` atribute.
        """
        self.schema = dict_to_bigquery_fields(self.schema)
        self.schema_type = "bigquery"
        return self

    def str(self) -> "Schema":
        """
        Convert a dictionary to a string representation of the schema.

        Returns:
        - `Schema`: The Schema object with string `schema` atribute.
        """
        self.schema = type_to_str(self.schema)
        self.schema_type = "str"
        return self


if __name__ == "__main__":
    d = {"a": [1, 2, 3], "b": ["a", "b", "c"], "c": [1, 2, 3]}
    inferred_schema = infer_schema(d)
    print(Schema(inferred_schema))
    exit()
    schema = {
        "a": float,
        "b": lambda x: x.upper(),
        "c": {1: "one", 2: "two", 3: "three"},
    }
    d = enforce_schema(d, schema)
    print(d)
