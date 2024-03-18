from gcp_tools import BigQuery
from concurrent.futures import ThreadPoolExecutor


def delete_test_bigquery_datasets():
    """
    Deletes all BigQuery datasets that start with "test_"
    """
    datasets = BigQuery().ls()
    datasets_to_delete = [d for d in datasets if d.startswith("test_")]
    del_fun = lambda x: BigQuery(dataset=x).delete()
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, datasets_to_delete)


if __name__ == "__main__":
    delete_test_bigquery_datasets()
