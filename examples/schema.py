"""
Example module to demonstrate how the Schema module is used in this library
"""


def example_translate():
    from gcp_tools import Schema

    schema = {"a": "int", "b": "string", "c": "float"}
    str_schema = Schema(schema).str()
    bigquery_schema = Schema(schema).bigquery()
    pyarrow_schema = Schema(schema).pyarrow()
    pandas_schema = Schema(schema).pandas()
    python_schema = Schema(schema).python()

    print(f"Schema: {schema}")
    print(f"String schema: {str_schema}")
    print(f"Pandas schema: {pandas_schema}")
    print(f"Pythong schema: {python_schema}")


def example_from_data():
    from gcp_tools import Schema

    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [None, 2.0, 3.0],
        "d": [None, False, True],
    }
    str_schema = Schema(data, is_data=True).str()
    bigquery_schema = Schema(data, is_data=True).bigquery()
    pyarrow_schema = Schema(data, is_data=True).pyarrow()
    pandas_schema = Schema(data, is_data=True).pandas()
    python_schema = Schema(data, is_data=True).python()

    print(f"Data: {data}")
    print(f"String schema: {str_schema}")
    print(f"Pandas schema: {pandas_schema}")
    print(f"Python schema: {python_schema}")

    # Application: Create empty BigQuery table with schema
    from gcp_tools import BigQuery

    table_id = "example_dataset.example_table"
    BigQuery(table_id).create(schema=bigquery_schema)
    BigQuery(table_id).insert(data)

    # Application: Apply schema to a DataFrame
    import pandas as pd

    df = pd.DataFrame(data)
    df = df.astype(pandas_schema)

    # Application: Apply schema to a PyArrow Table
    import pyarrow as pa

    table = pa.Table.from_pandas(df)
    table = table.cast(pyarrow_schema)


if __name__ == "__main__":
    example_from_data()
