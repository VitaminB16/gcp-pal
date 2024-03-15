import os
import logging
import collections.abc
from google.cloud import pubsub_v1, logging as gcp_logging

LIST_LIKE_TYPES = (list, tuple, set, frozenset, collections.abc.KeysView)

if os.getenv("PLATFORM", "GCP") in ["GCP", "local"]:
    client = gcp_logging.Client()
    client.get_default_handler()
    client.setup_logging()


def dtype_str_to_type(dtype_str):
    """
    Map a string representation of a dtype to its corresponding Python type
    or return the string if it's a Pandas-specific type.
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


def force_list(x):
    """
    Force x to be a list
    """
    if isinstance(x, LIST_LIKE_TYPES):
        return x
    else:
        return [x]


def is_series(obj):
    """Check if an object is a pandas Series without importing pandas."""
    return type(obj).__name__ == "Series"


def is_dataframe(obj):
    """Check if an object is a pandas DataFrame without importing pandas."""
    return type(obj).__name__ == "DataFrame"


def log(*args, **kwargs):
    """
    Function for logging to Google Cloud Logs. Logs a message as usual, and logs a dictionary of data as jsonPayload.

    Arguments:
        *args (list): list of elements to "print" to google cloud logs.
    """
    # Use these environment variables as payload to log to Google Cloud Logs
    env_keys = ["PLATFORM"]
    env_data = {key: os.getenv(key, None) for key in env_keys}
    log_data = {k: v for k, v in env_data.items() if v is not None}

    # If any arguments are a dictionary, add it to the log_data so it can be queried in Google Cloud Logs
    for arg in args:
        if isinstance(arg, dict):
            log_data.update(arg)
        log_data["message"] = " ".join([str(a) for a in args])

    if os.getenv("PLATFORM", "Local") in ["GCP"]:
        logging.info(log_data)
    else:
        # If running locally, use a normal print
        print(log_data["message"], **kwargs)


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
    if callable(schema):
        # For callable schemas, apply directly
        return series.apply(schema)
    elif isinstance(schema, dict):
        # For dictionary schemas, use map (assuming intention is to map values based on keys)
        return series.map(lambda x: schema.get(x, x))
    elif isinstance(schema, (type, str)):
        # For type schemas, cast the Series to the specified type
        return series.astype(schema)
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
    Enforce a schema on a dataframe or dictionary
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


if __name__ == "__main__":
    d = {"a": [1, 2, 3], "b": ["a", "b", "c"], "c": [1, 2, 3]}
    schema = {
        "a": float,
        "b": lambda x: x.upper(),
        "c": {1: "one", 2: "two", 3: "three"},
    }
    d = enforce_schema(d, schema)
    print(d)
