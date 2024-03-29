import pandas as pd
from uuid import uuid4
from pandas_gbq import read_gbq
from google.cloud import bigquery

from gcp_tools import BigQuery


# Basic utils
def list_tables(dataset):
    bq = bigquery.Client(location="europe-west2")
    try:
        tables = list(bq.list_tables(dataset))
    except Exception as e:
        return []
    table_ids = [table.table_id for table in tables]
    return table_ids


def list_datasets():
    bq = bigquery.Client(location="europe-west2")
    datasets = list(bq.list_datasets())
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    return dataset_ids


def table_exists(table_id):
    dataset, table = table_id.split(".")
    return table in list_tables(dataset)


def dataset_exists(dataset_id):
    return dataset_id in list_datasets()


def delete_table(table_id):
    bq = bigquery.Client(location="europe-west2")
    try:
        bq.delete_table(table_id)
    except:
        pass


def delete_dataset(dataset_id):
    bq = bigquery.Client(location="europe-west2")
    try:
        bq.delete_dataset(dataset_id, delete_contents=True)
    except:
        pass


# Tests
def test_BigQuery_init():
    bq = BigQuery("project.dataset.table")  # Testing
    assert bq.project == "project"
    assert bq.dataset == "dataset"
    assert bq.table == "table"
    bq = BigQuery("dataset.table", project="project")  # Testing
    assert bq.project == "project"
    assert bq.dataset == "dataset"
    assert bq.table == "table"
    bq = BigQuery("table", dataset="dataset", project="project")  # Testing
    assert bq.project == "project"
    assert bq.dataset == "dataset"
    assert bq.table == "table"
    failed_successfully = {}
    try:
        bq = BigQuery("dataset.tablename1.tablename2", project="project")  # Testing
        failed_successfully[0] = False
    except ValueError:
        failed_successfully[0] = True
    try:
        bq = BigQuery("table", project="project")  # Testing
        failed_successfully[1] = False
    except ValueError:
        failed_successfully[1] = True
    failed = [k for k, v in failed_successfully.items() if not v]
    assert not failed


def test_create_table():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    success1 = not table_exists(table_id)
    BigQuery(table_id).create_table()  # Testing: table created
    success2 = table_exists(table_id)
    delete_table(table_id)
    assert success1
    assert success2


def test_create_table_df():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    success1 = not table_exists(table_id)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).convert_dtypes()
    BigQuery(table_id).create_table(data=df)  # Testing: table created & data correct
    success2 = table_exists(table_id)
    queried_df = read_gbq(f"SELECT * FROM {table_id}")
    success3 = df.equals(queried_df)
    delete_table(table_id)
    assert success1
    assert success2
    assert success3


def test_delete_table():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    BigQuery(table_id).create_table()  # Testing: table created
    success1 = table_exists(table_id)
    BigQuery(table_id).delete_table()  # Testing: table deleted
    success2 = not table_exists(table_id)
    assert success1
    assert success2


def test_delete_dataset():
    dataset = f"test_dataset_{uuid4().hex}"
    bq = bigquery.Client(location="europe-west2")
    bq.create_dataset(dataset)
    success1 = dataset in list_datasets()
    BigQuery(dataset=dataset).delete_dataset()  # Testing: dataset deleted
    success2 = dataset not in list_datasets()
    assert success1
    assert success2


def test_delete():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    success1 = not table_exists(table_id)
    BigQuery(table_id).create_table()  # Testing table and dataset created
    success2 = table_exists(table_id)
    success3 = dataset_exists(dataset)
    BigQuery(table_id).delete()  # Testing: table deleted but not dataset
    success4 = not table_exists(table_id)
    success5 = dataset_exists(dataset)
    BigQuery(dataset=dataset).delete()  # Testing: dataset deleted
    success6 = not dataset_exists(dataset)
    assert success1
    assert success2
    assert success3
    assert success4
    assert success5
    assert success6


def test_all_BQ():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    success1 = not table_exists(table_id)

    BigQuery(table_id).create_table()  # Testing: table created
    success2 = table_exists(table_id)
    success3 = dataset in BigQuery().ls()  # Testing: dataset listed
    success4 = table_name in BigQuery(dataset=dataset).ls()  # Testing: table listed
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).convert_dtypes()
    BigQuery(table_id).insert(df)  # Testing: data inserted
    query = f"SELECT * FROM {table_id}"

    queried_df = BigQuery(table_id).query(query)  # Testing: data correct
    success5 = df.equals(queried_df)

    BigQuery(table_id).delete()  # Testing: table deleted
    success6 = not table_exists(table_id)

    BigQuery(dataset=dataset).delete()  # Testing: dataset deleted
    success7 = not dataset_exists(dataset)
    assert success1
    assert success2
    assert success3
    assert success4
    assert success5
    assert success6
    assert success7


def test_sql_builder():
    from gcp_tools.bigquery import SQLBuilder

    sql_builder = SQLBuilder("clean2.new_table")

    query1, params1 = sql_builder.select("name").where([("age", ">", 25)]).build()
    expected_query1 = "SELECT `name` FROM `clean2.new_table` WHERE `age` > @param_0"

    query2, params2 = (
        sql_builder.select("name")
        .where([("age", ">", 25), ("age", "<", 35)])
        .limit(10)
        .build()
    )
    expected_query2 = "SELECT `name` FROM `clean2.new_table` WHERE `age` > @param_0 AND `age` < @param_1 LIMIT 10"

    assert query1 == expected_query1
    assert params1 == {"param_0": 25}
    assert query2 == expected_query2
    assert params2 == {"param_0": 25, "param_1": 35}


def test_bq_read():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).convert_dtypes()
    BigQuery(table_id).create_table(data=df)  # Testing: table created & data correct
    queried_df1 = BigQuery(table_id).read()
    queried_df2 = BigQuery(table_id).read(limit=1)
    queried_df3 = BigQuery(table_id).read(columns=["a"])
    queried_df4 = BigQuery(table_id).read(columns=["a"], limit=1)
    queried_df5 = BigQuery(table_id).read(columns=["a"], filters=[("a", ">", 1)])
    queried_df6 = BigQuery(table_id).read(
        columns=["a"], filters=[("a", ">", 1), ("b", ">", 5)]
    )
    success1 = df.equals(queried_df1)
    success2 = df.head(1).equals(queried_df2)
    success3 = df[["a"]].equals(queried_df3)
    success4 = df[["a"]].head(1).equals(queried_df4)
    success5 = df[df["a"] > 1][["a"]].reset_index(drop=True).equals(queried_df5)
    success6 = (
        df[(df["a"] > 1) & (df["b"] > 5)][["a"]]
        .reset_index(drop=True)
        .equals(queried_df6)
    )
    assert success1
    assert success2
    assert success3
    assert success4
    assert success5
    assert success6


def test_get_table():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).convert_dtypes()
    BigQuery(table_id).create_table(data=df)
    table_ref = BigQuery(table_id).get_table()
    success1 = table_ref.table_id == table_name
    success2 = table_ref.dataset_id == dataset
    success3 = table_ref.num_rows == 3
    success4 = table_ref.num_bytes > 0
    delete_dataset(dataset)
    assert success1
    assert success2
    assert success3
    assert success4


def test_get_dataset():
    dataset = f"test_dataset_{uuid4().hex}"
    bq = bigquery.Client(location="europe-west2")
    bq.create_dataset(dataset)
    dataset_ref = BigQuery(dataset=dataset).get_dataset()
    print(dataset_ref.__dict__)
    success = dataset_ref.dataset_id == dataset
    delete_dataset(dataset_ref)
    assert success


def test_get():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).convert_dtypes()
    BigQuery(table_id).create_table(data=df)

    table_ref = BigQuery(table_id).get()  # Testing: get_table
    success1 = table_ref.table_id == table_name
    success2 = table_ref.dataset_id == dataset
    success3 = table_ref.num_rows == 3
    success4 = table_ref.num_bytes > 0

    dataset_ref = BigQuery(dataset=dataset).get()  # Testing: get_dataset
    success5 = dataset_ref.dataset_id == dataset

    delete_dataset(dataset)
    assert success1
    assert success2
    assert success3
    assert success4
    assert success5


def test_schema():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4.0, 5.1, 6.0], "c": ["a", "b", "c"]}
    ).convert_dtypes()
    BigQuery(table_id).create_table(data=df)
    schema_field = BigQuery(table_id).schema()
    success1 = schema_field[0].name == "a"
    success2 = schema_field[1].name == "b"
    success3 = schema_field[2].name == "c"
    success4 = schema_field[0].field_type == "INTEGER"
    success5 = schema_field[1].field_type == "FLOAT"
    success6 = schema_field[2].field_type == "STRING"
    success7 = len(schema_field) == 3

    delete_dataset(dataset)
    assert success1
    assert success2
    assert success3
    assert success4
    assert success5
    assert success6
    assert success7


def test_set_schema():
    success = {}
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    BigQuery(table_id).create_table()
    schema_before = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "INTEGER"),
    ]
    BigQuery(table_id).set_schema(schema_before)
    schema_field = BigQuery(table_id).schema()
    success[0] = schema_field[0].name == "a"
    success[1] = schema_field[1].name == "b"
    success[2] = schema_field[0].field_type == "INTEGER"
    success[3] = schema_field[1].field_type == "INTEGER"
    success[4] = len(schema_field) == 2

    schema_after = [
        bigquery.SchemaField("a", "INTEGER"),
        bigquery.SchemaField("b", "INTEGER"),
        bigquery.SchemaField("c", "STRING"),
    ]
    BigQuery(table_id).set_schema(schema_after)
    schema_field = BigQuery(table_id).schema()
    success[5] = schema_field[0].name == "a"
    success[6] = schema_field[1].name == "b"
    success[7] = schema_field[2].name == "c"
    success[8] = schema_field[0].field_type == "INTEGER"
    success[9] = schema_field[1].field_type == "INTEGER"
    success[10] = schema_field[2].field_type == "STRING"
    success[11] = len(schema_field) == 3

    delete_dataset(dataset)

    failed = [k for k, v in success.items() if not v]
    assert not failed


def test_write():
    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4.0, 5.1, 6.0], "c": ["a", "b", "c"]}
    ).convert_dtypes()
    BigQuery(table_id).write(df)
    queried_df = BigQuery(table_id).read()
    success = df.equals(queried_df)
    delete_dataset(dataset)
    assert success


def test_infer_uri():
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gcp_tools import Storage

    success = {}

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).convert_dtypes()
    partition_cols = ["a"]

    bucket_name = f"test_bucket_{uuid4().hex}"
    file_name = f"file.parquet"
    file_path = f"gs://{bucket_name}/{file_name}"
    Storage(bucket_name).create()

    pq.write_to_dataset(
        table=pa.Table.from_pandas(df),
        root_path=file_path,
        partition_cols=partition_cols,
    )

    inferred_uri, inferred_format, _ = BigQuery()._infer_uri(file_path)
    success[0] = inferred_uri == file_path + "*"
    success[1] = inferred_format == "PARQUET"

    file_name = f"file.csv"
    file_path = f"gs://{bucket_name}/{file_name}"
    with Storage(file_path).open("w") as f:
        df.to_csv(f, index=False)

    inferred_uri, inferred_format, _ = BigQuery()._infer_uri(file_path)
    success[2] = inferred_uri == file_path
    success[3] = inferred_format == "CSV"

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_exists():
    success = {}

    table_name = f"test_table_{uuid4().hex}"
    dataset = f"test_dataset_{uuid4().hex}"
    table_id = f"{dataset}.{table_name}"

    success[0] = not BigQuery(dataset=dataset).exists()
    success[1] = not BigQuery(table_id).exists()
    BigQuery(table_id).create_table()
    success[2] = BigQuery(dataset=dataset).exists()
    success[3] = BigQuery(table_id).exists()
    BigQuery(table_id).delete()
    success[4] = not BigQuery(table_id).exists()
    BigQuery(dataset=dataset).delete()
    success[5] = not BigQuery(dataset=dataset).exists()

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_create_extable():
    import pandas as pd
    from gcp_tools import Storage, Parquet
    from gcp_tools.schema import Schema

    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    pa_schema = Schema(data, is_data=True).pyarrow()
    bq_schema = Schema(data, is_data=True).bigquery()
    bucket_name = f"test_bucket_{uuid4().hex}"
    file_name = f"gs://{bucket_name}/file.parquet"

    Storage(bucket_name).create()

    Parquet(file_name).write(data, partition_cols=["a"], schema=pa_schema)
    success[0] = Storage(file_name).exists()
    success[1] = Storage(file_name).isdir()

    dataset_name = f"test_dataset_{uuid4().hex}"
    table_name = f"test_ext_table"
    table_id = f"{dataset_name}.{table_name}"
    BigQuery(table_id).create(file_name, schema=bq_schema)
    success[2] = BigQuery(table_id).exists()

    queried_df = BigQuery().read(file_name, schema=bq_schema)

    queried_df = queried_df.sort_values("a").reset_index(drop=True)
    queried_df = queried_df[data.columns]
    success[100] = data.equals(queried_df)
    success[101] = data.dtypes.equals(queried_df.dtypes)

    failed = [k for k, v in success.items() if not v]

    assert not failed
