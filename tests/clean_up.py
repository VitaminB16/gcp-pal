from gcp_tools import BigQuery, Firestore
from concurrent.futures import ThreadPoolExecutor


def delete_test_bigquery_datasets():
    """
    Deletes all BigQuery datasets that start with "test_"
    """
    datasets = BigQuery().ls()
    datasets_to_delete = [d for d in datasets if d.startswith("test_")]
    del_fun = lambda x: BigQuery(dataset=x).delete() if x.startswith("test_") else None
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, datasets_to_delete)


def delete_test_firestore_collections():
    """
    Deletes all Firestore collections that start with "test_"
    """
    collections = Firestore().ls()
    collections_to_delete = [c for c in collections if c.startswith("test_")]
    del_fun = lambda x: Firestore(x).delete() if x.startswith("test_") else None
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, collections_to_delete)


if __name__ == "__main__":
    delete_test_bigquery_datasets()
    delete_test_firestore_collections()
