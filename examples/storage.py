"""
Example function to demonstrate how to use the Storage module for Google Cloud Storage operations.
"""


def example_bucket_operations():
    from gcp_pal import Storage
    from uuid import uuid4

    bucket_name = f"example_bucket_{uuid4()}"

    # Create a bucket
    Storage(bucket_name).create()

    # List all buckets
    buckets = Storage().ls()
    print(buckets)

    # Delete the bucket
    Storage(bucket_name).delete()

    # List all buckets
    buckets = Storage().ls()
    print(buckets)


def example_upload_download():
    from gcp_pal import Storage
    from uuid import uuid4

    bucket_name = f"example_bucket_{uuid4()}"
    file_name = "hello_world.txt"
    destination_path = f"{bucket_name}/{file_name}"

    # Upload a file
    Storage(destination_path).upload(contents="Hello, World!")

    # Download a file to a variable
    contents = Storage(destination_path).download()
    print(contents)

    # Download a file to a local file
    Storage(destination_path).download(local_path="examples/downloads/hello_world.txt")

    with open("examples/downloads/hello_world.txt", "r") as file:
        contents = file.read()
    print(contents)

    # Delete the bucket
    Storage(bucket_name).delete()


def example_parquet_read_write():
    import pandas as pd
    from uuid import uuid4
    from gcp_pal import Storage

    bucket_name = f"example_bucket_{uuid4()}"
    file_name = "example.parquet"
    destination_path = f"{bucket_name}/{file_name}"
    df = pd.DataFrame(
        {
            "A": [1, 2, 3, 4],
            "B": ["a", "b", "c", "d"],
            "C": [1.1, 2.2, 3.3, 4.4],
        }
    )

    # Write a DataFrame to a single Parquet file (no partition columns)
    Storage(destination_path).write(df)

    # Read the Parquet file
    read_df = Storage(destination_path).read()
    print(read_df)

    # Delete the file
    Storage(destination_path).delete()

    # Write a DataFrame to a partitioned Parquet file
    Storage(destination_path).write(df, partition_cols=["A", "B"])

    # Read the Parquet file
    read_df = Storage(destination_path).read()
    print(read_df)

    # Delete the bucket
    Storage(bucket_name).delete()


if __name__ == "__main__":
    example_parquet_read_write()
