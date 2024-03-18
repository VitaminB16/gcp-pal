import pandas as pd
from uuid import uuid4
from pandas_gbq import read_gbq
from google.cloud import bigquery

from gcp_tools import BigQuery


# Basic utils
def list_tables(dataset):
    bq = bigquery.Client()
    try:
        tables = list(bq.list_tables(dataset))
    except Exception as e:
        return []
    table_ids = [table.table_id for table in tables]
    return table_ids


def list_datasets():
    bq = bigquery.Client()
    datasets = list(bq.list_datasets())
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    return dataset_ids


def table_exists(table_id):
    dataset, table = table_id.split(".")
    return table in list_tables(dataset)


def dataset_exists(dataset_id):
    return dataset_id in list_datasets()


def delete_table(table_id):
    bq = bigquery.Client()
    try:
        bq.delete_table(table_id)
    except:
        pass


def delete_dataset(dataset_id):
    bq = bigquery.Client()
    try:
        bq.delete_dataset(dataset_id, delete_contents=True)
    except:
        pass


# # Tests
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
    bq = bigquery.Client()
    bq.create_table(table_id, exists_ok=True)
    success1 = table_exists(table_id)
    BigQuery(table_id).delete_table()  # Testing: table deleted
    success2 = not table_exists(table_id)
    assert success1
    assert success2


def test_delete_dataset():
    dataset = f"test_dataset_{uuid4().hex}"
    bq = bigquery.Client()
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
