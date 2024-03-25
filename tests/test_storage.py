from gcp_tools import Storage


def test_storage_init():
    assert Storage("bucket_name").bucket_name == "bucket_name"
    assert Storage("gs://bucket_name").bucket_name == "bucket_name"
    assert Storage("bucket_name/file").bucket_name == "bucket_name"
    assert Storage("gs://bucket_name/file").bucket_name == "bucket_name"
    assert Storage("gs://bucket_name").path == "gs://bucket_name"
    assert Storage("bucket_name").path == "gs://bucket_name"
    assert Storage("bucket_name/file").path == "gs://bucket_name/file"
    assert Storage("gs://bucket_name/file").path == "gs://bucket_name/file"
    assert Storage(bucket_name="bucket").bucket_name == "bucket"
    assert Storage(bucket_name="bucket").path == "gs://bucket"
    assert Storage().path == "gs://"
    assert Storage().bucket_name is None
