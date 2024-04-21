"""
Example module to demonstrate how to use the Parquet module for Google Cloud Storage operations.
"""


def example_parquet_read_write():
    from gcp_pal import Storage, Parquet
    from uuid import uuid4
    import pandas as pd

    bucket_name = f"example_bucket_{uuid4()}"
    file_name = "example.parquet"
    destination_path = f"gs://{bucket_name}/{file_name}"

    data = pd.DataFrame(
        {
            "A": [1, 2, 3, 4],
            "B": ["a", "b", "c", "d"],
            "C": [1.1, 2.2, 3.3, 4.4],
        }
    )
    Storage(bucket_name).create()

    # Write a DataFrame to a partitioned Parquet file
    Parquet(destination_path).write(data, partition_cols=["A", "B"])

    # Read the Parquet file
    read_df = Parquet(destination_path).read()

    # Delete the bucket
    Storage(bucket_name).delete()

    print(read_df)


if __name__ == "__main__":
    example_parquet_read_write()
