from gcp_tools import (
    BigQuery,
    Firestore,
    Storage,
    CloudFunctions,
    CloudRun,
    SecretManager,
)
from concurrent.futures import ThreadPoolExecutor


def delete_test_bigquery_datasets():
    """
    Deletes all BigQuery datasets that start with "test_"
    """
    datasets = BigQuery().ls()
    datasets_to_delete = [
        d
        for d in datasets
        if d.startswith("test_") or d.startswith("temp_") or d.startswith("example_")
    ]
    del_fun = lambda x: (
        BigQuery(dataset=x).delete()
        if x.startswith("test_") or x.startswith("temp_") or x.startswith("example_")
        else None
    )
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, datasets_to_delete)


def delete_test_firestore_collections():
    """
    Deletes all Firestore collections that start with "test_"
    """
    collections = Firestore().ls()
    collections_to_delete = [
        c
        for c in collections
        if c.startswith("test_") or c.startswith("temp_") or c.startswith("example_")
    ]
    del_fun = lambda x: (
        Firestore(x).delete()
        if x.startswith("test_") or x.startswith("temp_") or x.startswith("example_")
        else None
    )
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, collections_to_delete)


def delete_test_storage_buckets():
    """
    Deletes all Storage buckets that start with "test_"
    """
    buckets = Storage().ls()
    buckets_to_delete = [
        b
        for b in buckets
        if b.startswith("test_") or b.startswith("temp_") or b.startswith("example_")
    ]
    del_fun = lambda x: Storage(x).delete()
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, buckets_to_delete)
    buckets = Storage().ls()
    buckets_to_delete = [
        b
        for b in buckets
        if b.startswith("test_") or b.startswith("temp_") or b.startswith("example_")
    ]
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, buckets_to_delete)


def delete_test_cloud_functions():
    """
    Deletes all Cloud Functions that start with "test_"
    """
    functions = CloudFunctions().ls()
    functions_to_delete = [
        f
        for f in functions
        if f.startswith("test_") or f.startswith("temp_") or f.startswith("example_")
    ]
    del_fun = lambda x: CloudFunctions(x).delete()
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, functions_to_delete)


def delete_test_cloud_run_services():
    """
    Deletes all Cloud Run services that start with "test_"
    """
    services = CloudRun().ls()
    services_to_delete = [
        s
        for s in services
        if s.startswith("test_") or s.startswith("temp_") or s.startswith("example_")
    ]
    del_fun = lambda x: CloudRun(x).delete()
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, services_to_delete)


def delete_test_secret_manager_secrets():
    """
    Deletes all Secret Manager secrets that start with "test_"
    """
    secrets = SecretManager().ls()
    secrets_to_delete = [
        s
        for s in secrets
        if s.startswith("test_") or s.startswith("temp_") or s.startswith("example_")
    ]
    del_fun = lambda x: SecretManager(x).delete()
    with ThreadPoolExecutor() as executor:
        executor.map(del_fun, secrets_to_delete)


if __name__ == "__main__":
    delete_test_bigquery_datasets()
    delete_test_firestore_collections()
    delete_test_storage_buckets()
    delete_test_cloud_functions()
    delete_test_cloud_run_services()
    delete_test_secret_manager_secrets()
