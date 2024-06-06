import os
from gcp_pal.utils import try_import

from typing import List
from urllib.parse import unquote

from gcp_pal.utils import is_dataframe, force_list, ModuleHandler


class Parquet:
    """
    A class for working with Parquet files in Google Cloud Storage.
    """

    def __init__(self, path: str, **kwargs):
        self.path = path
        self.pa = ModuleHandler("pyarrow").please_import(who_is_calling="Parquet")
        self.pq = ModuleHandler("pyarrow.parquet").please_import(
            who_is_calling="Parquet"
        )

    def write(
        self,
        data,
        partition_cols: List[str] = None,
        schema=None,
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
            from gcp_pal.schema import Schema

            schema = Schema(data).pyarrow()
        if partition_cols:
            partition_cols = force_list(partition_cols)
            if set(partition_cols) == set(data.columns):
                msg = (
                    f"Partition columns {partition_cols} are the same as data columns."
                )
                msg += "\nThis might cause unexpected behaviour with BigQuery external tables."
                # msg += "\nAdding an `__index` column to the data."
                # log(msg)
                # data["__index"] = range(len(data))
                # if schema:
                #     schema = schema.append(pa.field("__index", pa.int64()))
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
            self.pq.write_to_dataset(
                table=self.pa.Table.from_pandas(data),
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
            from gcp_pal.schema import Schema

            df = _get_partitions_df(self.path)
            schema = Schema(schema).pandas()
            df = df.astype(schema)
        else:
            if isinstance(schema, dict):
                from gcp_pal.schema import Schema

                schema = Schema(schema).pyarrow()
            try:
                df = self.pq.read_table(
                    self.path,
                    columns=columns,
                    filters=filters,
                    schema=schema,
                    **kwargs,
                ).to_pandas()
            except FileNotFoundError:
                try_import("pandas")
                import pandas as pd

                df = pd.DataFrame()
        if df.empty and not allow_empty:
            raise ValueError(
                f"File {self.path} is empty or not found, and allow_empty is False"
            )
        if "__index" in df.columns:
            df = df.drop(columns="__index")
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
    try_import("pandas", "parquet._get_partitions_df")
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
    from gcp_pal import Storage

    partition_cols = _get_partition_cols(path)
    glob_query = os.path.join(path, *["*"] * len(partition_cols))
    all_partitions = Storage().glob(glob_query)
    return all_partitions


def _get_partition_cols(path):
    """
    Get the partitions of the parquet file without reading the files
    """
    from gcp_pal import Storage

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
    from gcp_pal import Storage
    from gcp_pal.schema import Schema

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
