import os
import pyarrow as pa
from typing import List
import pyarrow.parquet as pq
from urllib.parse import unquote


from gcp_tools import BigQuery
from gcp_tools.utils import log, is_dataframe, force_list


class Parquet:
    """
    A class for working with Parquet files in Google Cloud Storage.
    """

    def __init__(self, path: str, **kwargs):
        self.path = path

    def write(
        self,
        data,
        partition_cols: List[str] = None,
        schema: pa.Schema = None,
        basename_template="{i}.parquet",
        **kwargs,
    ):
        """
        Write a DataFrame to a Parquet file in Google Cloud Storage.

        Args:
        - data: The data to write.
        - partition_cols: The columns to partition by.
        - schema: The schema to write.
        - basename_template: The template for the Parquet file names.
        - kwargs: Additional arguments to pass to `pyarrow` or `pandas`.
        """
        if schema is None:
            from gcp_tools.schema import Schema

            schema = Schema(data).infer_schema().pyarrow()
        if partition_cols:
            return self._write_partitioned(
                data=data,
                partition_cols=partition_cols,
                schema=schema,
                basename_template=basename_template,
                **kwargs,
            )
        else:
            return self._write_single(data, schema=schema, **kwargs)

    def _write_partitioned(
        self, data, partition_cols, schema, basename_template, **kwargs
    ):
        """
        Write a DataFrame to a partitioned Parquet file in Google Cloud Storage.

        Args:
        - data: The data to write.
        - partition_cols: The columns to partition by.
        - schema: The schema to write.
        - basename_template: The template for the Parquet file names.
        - kwargs: Additional arguments to pass to `pyarrow.parquet.write_to_dataset`.
        """
        if is_dataframe(data):
            pq.write_to_dataset(
                table=pa.Table.from_pandas(data),
                root_path=self.path,
                partition_cols=partition_cols,
                schema=schema,
                basename_template=basename_template,
                **kwargs,
            )
        else:
            raise ValueError(f"Data of type {type(data)} is not supported yet.")

    def _write_single(self, data, schema, **kwargs):
        """
        Write a DataFrame to a single Parquet file in Google Cloud Storage.

        Args:
        - data: The data to write.
        - schema: The schema to write.
        - kwargs: Additional arguments to pass to `pyarrow.parquet.write_table`.
        """
        if is_dataframe(data):
            return data.to_parquet(
                self.path,
                index=False,
                engine="pyarrow",
                **kwargs,
            )
        else:
            raise ValueError(f"Data of type {type(data)} is not supported yet.")

    def read(
        self,
        allow_empty=True,
        schema=None,
        filters=None,
        columns=None,
        read_partitions_only=False,
        **kwargs,
    ):
        """
        Read a Parquet file from Google Cloud Storage.

        Args:
        - allow_empty: Whether to allow empty data.
        - schema: The schema to read.
        - filters: The filters to apply.
        - use_bigquery: Whether to use BigQuery.
        - columns: The columns to read.
        - read_partitions_only: If true, it will read the data from partition names only using `glob`.
        """
        if read_partitions_only:
            from gcp_tools.schema import Schema

            df = _get_partitions_df(self.path)
            schema = Schema(schema).pandas()
            df = df.astype(schema)
        else:
            try:
                df = pq.read_table(
                    self.path,
                    columns=columns,
                    filters=filters,
                    schema=schema,
                    **kwargs,
                ).to_pandas()
            except FileNotFoundError:
                import pandas as pd

                df = pd.DataFrame()
        if df.empty and not allow_empty:
            raise ValueError(
                f"File {self.path} is empty or not found, and allow_empty is False"
            )
        df = self.fix_column_types(df, filters)

        return df

    def generate_filters(self, filters: dict = None):
        """
        Generate the filters

        Args:
        - filters: The filters to apply. If the filters are a dictionary, it will be converted to a list of tuples.

        Returns:
        - list[tuple]

        Examples:
        >>> Parquet().generate_filters({"a": [1, 2], "b": [3, 4], "c": 5})
        [('a', 'in', [1, 2]), ('b', 'in', [3, 4]), ('c', 'in', [5])]
        """
        if filters is None:
            return None
        if isinstance(filters, list):
            return filters

        file_filters = []
        for column, value in filters.items():
            value = force_list(value)
            file_filters.append((column, "in", value))
        return file_filters

    def fix_column_types(self, df, filters, replace_underscore=True):
        """
        Ensure nothing strange happens with the column types of the df.

        Args:
        - df: The DataFrame to fix.
        - filters: The filters to apply.
        - replace_underscore: Whether to replace underscores with spaces in string columns.

        Returns:
        - pd.DataFrame

        Examples:
        >>> Parquet().fix_column_types(df, filters={"a": [1, 2], "b": [3, 4], "c": 5})
        """
        if (not filters) or df.empty:
            return df
        for c, c_type in [(x[0], type(x[1])) for x in filters]:
            df[c] = df[c].astype(c_type)
            if replace_underscore and (c_type == str):
                df[c] = df[c].str.replace("_", " ")
        return df


def _get_partitions_df(path):
    """
    Get the partitions of the parquet file
    """
    import pandas as pd

    all_partition_paths = _get_all_partition_paths(path)
    all_partitions = [x.split("/") for x in all_partition_paths]
    all_partitions = [dict(y.split("=") for y in x if "=" in y) for x in all_partitions]
    df = pd.DataFrame(all_partitions)
    df = df.map(unquote)
    return df


def _get_all_partition_paths(path):
    """
    Get the partitions of the parquet file
    """
    from gcp_tools import Storage

    partition_cols = _get_partition_cols(path)
    glob_query = os.path.join(path, *["*"] * len(partition_cols))
    all_partitions = Storage().glob(glob_query)
    return all_partitions


def _get_partition_cols(path):
    """
    Get the partitions of the parquet file without reading the files
    """
    from gcp_tools import Storage

    glob_query = os.path.join(path, "*")
    all_paths = Storage().glob(glob_query)
    while all_paths:
        glob_query = all_paths[0] + "/*"
        all_paths = Storage().glob(glob_query)
    glob_query = glob_query.split("/")
    partition_path = [x.split("=")[0] for x in glob_query if "=" in x]
    return partition_path


if __name__ == "__main__":
    import pandas as pd
    from gcp_tools import Storage
    from gcp_tools.schema import Schema

    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    schema = Schema(data).pyarrow()
    bucket_name = f"test_bucket_0b759c89-d519-4f14-971e-cc09727e5266"
    file_name = f"gs://{bucket_name}/file.parquet"
    # Storage().create_bucket(bucket_name)
    # Parquet(file_name).write(data, partition_cols=["a", "b"])

    read_df = Parquet(file_name).read(read_partitions_only=True, schema=schema)
    print(read_df)
    print(data)
    print(read_df.dtypes)
    print(data.dtypes)
