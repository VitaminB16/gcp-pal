from gcp_tools import BigQuery


def test_project_table_dataset():
    bq = BigQuery("project.dataset.table")
    assert bq.project == "project"
    assert bq.dataset == "dataset"
    assert bq.table == "table"
    bq = BigQuery("dataset.table", project="project")
    assert bq.project == "project"
    assert bq.dataset == "dataset"
    assert bq.table == "table"
    bq = BigQuery("table", dataset="dataset", project="project")
    assert bq.project == "project"
    assert bq.dataset == "dataset"
    assert bq.table == "table"
