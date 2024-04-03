"""
Example functions to demonstrate how to use the BigQuery module for creating a dataset, table, inserting data,
"""

from uuid import uuid4


def example_table():
    """
    Example function to demonstrate how to use the BigQuery module for creating a dataset,
        table, inserting data, querying the table, and deleting the table and dataset.
    """
    from gcp_tools import BigQuery, Schema
    from datetime import datetime

    # List all datasets in the project
    listed_datasets = BigQuery().ls()

    # BigQuery.create will create this dataset if it does not exist
    dataset_name = "example_dataset"
    # BigQuery.create will create this table in the dataset if it does not exist
    table_name = "example_table"

    table_id = f"{dataset_name}.{table_name}"
    schema = {
        "name": str,
        "age": int,
        "income": float,
        "is_student": bool,
        "created_at": datetime,
        "details": {
            "address": str,
            "phone": str,
        },
    }
    bq_schema = Schema(schema).bigquery()
    BigQuery(dataset=dataset_name, table=table_name).create(schema=bq_schema)

    # Insert data into the table
    data = [
        {
            "name": "Alice",
            "age": 25,
            "income": 50000.0,
            "is_student": False,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "details": {
                "address": "123 Main St",
                "phone": "555-1234",
            },
        },
        {
            "name": "Bob",
            "age": 30,
            "income": 20000.0,
            "is_student": True,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "details": {
                "address": "456 Elm St",
                "phone": "555-5678",
            },
        },
    ]
    # `table = table_id` will be parsed project (if given), dataset (if given) and table_name
    BigQuery(table_id).insert(data)

    # Query the table
    query = f"""
        SELECT name, age, income, is_student, created_at, details
        FROM {table_id}
        WHERE name IN ('Alice', 'Bob')
        AND age >= 25
        LIMIT 10
    """
    result = BigQuery().query(query, to_dataframe=True)
    # Equivalently, the `read` method will construct the same query as above.
    result = BigQuery(table_id).read(
        columns=["name", "age", "income", "is_student", "created_at", "details"],
        filters=[{"name": ["Alice", "Bob"]}, ("age", ">=", 25)],
        limit=10,
        to_dataframe=True,
    )

    print(result)

    # Delete the table
    BigQuery(dataset=dataset_name, table=table_name).delete()

    # Delete the dataset
    BigQuery(dataset=dataset_name).delete()


def example_external_table():
    from gcp_tools import BigQuery, Schema
    from examples.fixtures import write_external_csv_file

    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "income": [50000.0, 20000.0, 30000.0],
        "is_student": [None, True, False],
    }
    bucket_name = f"example_bucket_{uuid4()}"
    file_name = "example_data.csv"
    file_path = f"{bucket_name}/{file_name}"
    write_external_csv_file(data, bucket_name, file_name)

    bq_schema = Schema(data, is_data=True).bigquery()

    dataset_name = "example_dataset"
    table_name = "example_external_table"
    table_id = f"{dataset_name}.{table_name}"

    # Create since the file_path is a string, it will be treated as the path to the external data source
    BigQuery(table_id).create(file_path, schema=bq_schema)

    # Query the table
    result = BigQuery(table_id).read(to_dataframe=True)

    print(result)


def example_table_from_data():
    """
    Example function to demonstrate how to use the BigQuery module for creating a table directly from data.
    """
    from gcp_tools import BigQuery

    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "income": [50000.0, 20000.0, 30000.0],
        "is_student": [None, None, False],
    }

    dataset_name = "example_dataset"
    table_name = "example_table_from_data"
    table_id = f"{dataset_name}.{table_name}"

    # Using infer_schema=True will infer the schema from the data
    BigQuery(table_id).create(data, infer_schema=True)

    # Query the table
    result = BigQuery(table_id).read(to_dataframe=True, limit=10)

    print(result)

    BigQuery(table_id).delete()


if __name__ == "__main__":
    example_table_from_data()
