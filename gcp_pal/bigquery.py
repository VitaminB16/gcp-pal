from __future__ import annotations

import os
from gcp_pal.utils import try_import

from gcp_pal.schema import dict_to_bigquery_fields, Schema, dict_to_bigquery_fields
from gcp_pal.schema import bigquery_fields_to_dict
from gcp_pal.utils import (
    is_dataframe,
    get_auth_default,
    orient_dict,
    log,
    ClientHandler,
    ModuleHandler,
)


class SQLBuilder:
    """
    Class for safely building SQL queries for use with Google BigQuery.
    """

    ALLOWED_OPERATIONS = {"=", ">", "<", ">=", "<=", "IN", "LIKE"}

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.query_parts = ["FROM", f"`{table_name}`"]
        self.parameters = {}  # Holds parameters to prevent SQL injection

    def build(self) -> str:
        """
        Return the final query string and parameters.
        """
        sql_query = " ".join(self.query_parts), self.parameters

        # Reset query parts and parameters
        self.query_parts = ["FROM", f"`{self.table_name}`"]
        self.parameters = {}
        return sql_query

    def select(self, columns=None) -> "SQLBuilder":
        """
        Build a SELECT query.
        """
        if columns is None:
            columns = "*"
        if isinstance(columns, str):
            columns = [columns]
        columns = [f"`{x}`" for x in columns if x != "*"] or ["*"]
        select_clause = "SELECT " + ", ".join(columns)
        self.query_parts.insert(0, select_clause)  # Insert at the beginning
        return self

    def where(self, filters: list[tuple]) -> "SQLBuilder":
        """
        Build a WHERE clause with safe parameter handling.
        """
        if not filters:
            return self

        where_clause, params = self._where(filters)
        if where_clause:
            self.query_parts.append("WHERE " + where_clause)
            self.parameters.update(params)
        return self

    def limit(self, limit: int) -> "SQLBuilder":
        """
        Build a LIMIT clause.
        """
        if limit is not None and isinstance(limit, int) and limit > 0:
            self.query_parts.append(f"LIMIT {limit}")
        return self

    def _where(self, filters: list[tuple]) -> tuple:
        """
        Helper to build a WHERE clause safely.
        """
        conditions = []
        params = {}
        for i, filter in enumerate(filters):
            if isinstance(filter, dict):
                col, value = list(filter.items())[0]
                op = "IN"
            else:
                col, op, value = filter
            self._check_illegal_filters(col, op, value)
            param_name = f"param_{i}"
            if isinstance(value, list):
                placeholders = ", ".join(
                    [f"@{param_name}_{j}" for j, _ in enumerate(value)]
                )
                conditions.append(f"`{col}` {op} ({placeholders})")
                for j, v in enumerate(value):
                    params[f"{param_name}_{j}"] = v
            else:
                conditions.append(f"`{col}` {op} @{param_name}")
                params[param_name] = value
        return " AND ".join(conditions), params

    def _check_illegal_filters(self, col, op, value):
        """
        Helper to check for illegal characters in filters.
        """
        op = op.upper()
        if op not in self.ALLOWED_OPERATIONS:
            raise ValueError(f"Filter operator not allowed: {op}")
        if "`" in col:
            raise ValueError("Column or value contains illegal character: `")
        if "--" in col:
            raise ValueError("Column or value contains illegal character: --")
        if isinstance(value, str) and "--" in value:
            raise ValueError("Column or value contains illegal character: --")
        if isinstance(value, str) and "`" in value:
            raise ValueError("Column or value contains illegal character: `")


class BigQuery:
    """
    Class for operating Google BigQuery.
    """

    def __init__(self, table=None, dataset=None, project=None, location="europe-west2"):
        """
        Initializes the BigQuery client.

        Args:
        - table (str): Table or table ID for the BigQuery service.
        - dataset (str): Dataset or dataset ID for the BigQuery service.
        - project (str): Project ID for the BigQuery service.
        - location (str): Location for the BigQuery service (default: "europe-west2").

        Examples:
        - `BigQuery().query("SELECT * FROM project.dataset.table")` -> Executes a query and returns the results.
        - `BigQuery("dataset.table").insert(data)` -> Inserts data into the specified BigQuery table.
        - `BigQuery("dataset.new_table").create_table(schema=schema)` -> Creates a new table with the specified schema.

        Storage-equivalent examples:
        - `BigQuery().ls()` -> Lists all datasets in the project.
        - `BigQuery("dataset").ls()` -> Lists all tables in the specified dataset.
        - `BigQuery("dataset.table").read()` -> Reads the entire table.
        - `BigQuery("dataset.table").write(data)` -> Writes data to the specified table.
        - `BigQuery("dataset.table").delete()` -> Deletes the specified table.
        - `BigQuery("dataset").delete()` -> Deletes the specified dataset.
        """
        self.table = table
        self.dataset = dataset
        self.location = location
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        if not project:
            try:
                # E.g. BigQuery(table="project.dataset.table")
                self.project, self.dataset, self.table = table.split(".")
            except (ValueError, AttributeError):
                pass
        try:
            # E.g. BigQuery(table="dataset.table", project="project")
            self.dataset, self.table = self.table.split(".")
        except (ValueError, AttributeError):
            pass
        if self.table and "." in self.table:
            raise ValueError("Table name cannot contain '.'")
        if self.dataset and "." in self.dataset and not project:
            # E.g. BigQuery(dataset="project.dataset")
            self.project, self.dataset = self.dataset.split(".", 1)

        # table_id is the full table ID, e.g. "project.dataset.table"
        self.table_id = None
        if self.table:
            self.table_id = f"{self.dataset}.{self.table}"
        if self.project and self.table_id:
            self.table_id = f"{self.project}.{self.table_id}"
        if self.project and self.dataset:
            self.dataset_id = f"{self.project}.{self.dataset}"

        if not self.project:
            raise ValueError("Project ID not specified.")
        if not self.dataset and self.table:
            raise ValueError("Table name specified without dataset.")

        self.bigquery = ModuleHandler("google.cloud").please_import(
            "bigquery", who_is_calling="BigQuery"
        )
        self.client = ClientHandler(self.bigquery.Client).get(
            project=self.project, location=self.location
        )
        self.exceptions = ModuleHandler("google.api_core.exceptions").please_import(
            who_is_calling="BigQuery"
        )

    def __repr__(self):
        return f"BigQuery({self.table_id})"

    def query(self, sql, job_config=None, schema=None, to_dataframe=False, params=None):
        """
        Executes a query against BigQuery.

        Args:
        - sql (str): SQL query string.
        - job_config (bigquery.QueryJobConfig): Optional configuration for the query job.
        - to_dataframe (bool): If True, returns the results as a pandas DataFrame.
        - params (dict): Optional parameters to pass to the query.

        Returns:
        - Query results as a pandas DataFrame or BigQuery job object.
        """
        if params:
            job_config = self._parse_query_params(params, job_config=job_config)
        output = self.client.query(sql, job_config=job_config)
        if to_dataframe:
            try_import("pandas", "BigQuery.query.to_dataframe")

            output = output.to_dataframe().convert_dtypes()
            if schema and is_dataframe(output):
                pd_schema = Schema(schema).pandas()
                output = output.astype(pd_schema)
        else:
            output = output.result()
            try:
                output = list(output)
            except Exception as e:
                log(f"BigQuery - Error converting query results to list: {e}")
        sql_print = sql
        if params:
            for key, value in params.items():
                sql_print = sql_print.replace(f"@{key}", str(value))
        log(f"BigQuery - Query executed: \n{sql_print}")
        return output

    def read_table(
        self,
        job_config=None,
        to_dataframe=True,
        columns=None,
        filters=None,
        schema=None,
        limit=None,
    ):
        """
        Reads entire table from BigQuery.

        Args:
        - job_config (bigquery.QueryJobConfig): Optional configuration for the query job.
        - to_dataframe (bool): If True, returns the results as a pandas DataFrame.
        - columns (list): Columns to select.
        - filters (list): Filters to apply.
        - schema (list): Schema to apply.
        - limit (int): Limit the number of rows to return.

        Returns:
        - Query results as a pandas DataFrame or BigQuery job object.
        """
        sql, params = (
            SQLBuilder(self.table_id)
            .select(columns)
            .where(filters)
            .limit(limit)
            .build()
        )
        return self.query(
            sql=sql,
            job_config=job_config,
            to_dataframe=to_dataframe,
            schema=schema,
            params=params,
        )

    def read(
        self,
        filepath=None,
        job_config=None,
        to_dataframe=False,
        columns=None,
        filters=None,
        schema=None,
        limit=None,
    ):
        """
        Reads entire table from BigQuery.
        If the Google Storage filepath is provided, it will create an external table and read from it.
        This is generally faster than reading the data from storage using e.g. Pyarrow or Pandas.

        Args:
        - filepath (str): The Google Storage filepath to read from.
        - job_config (bigquery.QueryJobConfig): Optional configuration for the query job.
        - to_dataframe (bool): If True, returns the results as a pandas DataFrame.
        - columns (list): Columns to select.
        - filters (list): Filters to apply.
        - schema (list): Schema to apply.
        - limit (int): Limit the number of rows to return.

        Returns:
        - Query results as a pandas DataFrame or BigQuery job object.
        """
        if filepath is not None and isinstance(filepath, str):
            random_dataset = f"temp_dataset_{os.urandom(8).hex()}"
            random_table = f"temp_table_{os.urandom(8).hex()}"
            table_id = f"{random_dataset}.{random_table}"
            BigQuery(table_id).create_external_table(filepath, schema=schema)
            output = BigQuery(table_id).read_table(
                job_config=job_config,
                to_dataframe=to_dataframe,
                columns=columns,
                filters=filters,
                schema=schema,
                limit=limit,
            )
            BigQuery(dataset=random_dataset).delete()
            return output
        else:
            return self.read_table(
                job_config=job_config,
                to_dataframe=to_dataframe,
                columns=columns,
                filters=filters,
                schema=schema,
                limit=limit,
            )

    def _parse_query_params(self, params, job_config=None):
        """
        Helper to parse query parameters into bigquery.QueryJobConfig.

        Args:
        - params (dict): Query parameters to parse.
        - job_config (bigquery.QueryJobConfig): Optional configuration for the query job.

        Returns:
        - bigquery.QueryJobConfig object: The updated job_config.
        """
        if not job_config:
            job_config = self.bigquery.QueryJobConfig()

        query_params = []

        for key, value in params.items():
            val_type = {"int": "INTEGER", "float": "FLOAT", "str": "STRING"}.get(
                type(value).__name__, "STRING"
            )
            if isinstance(value, list):
                bq_param = self.bigquery.ArrayQueryParameter(key, val_type, value)
            else:
                bq_param = self.bigquery.ScalarQueryParameter(key, val_type, value)
            query_params.append(bq_param)
        job_config.query_parameters = job_config.query_parameters + query_params
        return job_config

    def insert(self, data, schema=None):
        """
        Inserts data into a BigQuery table.

        Args:
        - data (list of dicts | pandas.DataFrame): The data to insert.
        - schema (list of bigquery.SchemaField): Optional schema definition. Required if the table does not exist.

        Returns:
        - True if successful.

        Examples:
        - BigQuery("dataset.table").insert([{"a": 1, "b": "test"}])
        - BigQuery("dataset.table").insert(pd.DataFrame({"a": [1], "b": ["test"]}))
        """
        if is_dataframe(data):
            try_import("pandas_gbq", "BigQuery.insert.dataframe")
            from pandas_gbq import to_gbq

            return to_gbq(
                data,
                destination_table=self.table_id,
                if_exists="append",
                location=self.location,
            )

        if isinstance(data, dict):
            data = orient_dict(data, orientation="index")

        if not schema and isinstance(data, list):
            table = self.client.get_table(self.table_id)  # Make sure the table exists
            errors = self.client.insert_rows_json(table, data)
        elif schema:
            table = self.bigquery.Table(self.table_id, schema=schema)
            errors = self.client.insert_rows_json(table, data)
        else:
            raise ValueError(
                "Data format not supported or schema required for new table."
            )

        if errors == []:
            log(f"BigQuery - Data inserted into: {self.table}")
            return True
        else:
            log(
                f"BigQuery - Errors occurred while inserting data into: {self.table} -- {errors}"
            )
            return False

    def write(self, data, schema=None):
        """
        Writes data to a BigQuery table. If the table does not exist, it will be created. If the table exists, the data will be appended.

        Args:
        - data (list of dicts | pandas.DataFrame): The data to insert.
        - schema (list of bigquery.SchemaField): Optional schema definition. Required if the table does not exist.

        Returns:
        - True if successful.
        """
        if is_dataframe(data):
            self._create_table(data, schema=schema, exists_ok=True, if_exists="append")
            log(f"BigQuery - DataFrame written to {self.table}, schema: {schema}")
            return True

        if isinstance(data, dict):
            data = [data]

        if not schema:
            schema = Schema(data, is_data=True).bigquery()

        try:
            success = self.insert(data, schema=schema)
        except self.exceptions.NotFound:
            success = self.create_table(data, schema=schema, exists_ok=True)
        log(f"BigQuery - Data written to {self.table}, schema: {schema}")
        return success

    def _create_table(
        self,
        data=None,
        schema=None,
        exists_ok=True,
        if_exists=None,
        time_partition_col=None,
        range_partition_col=None,
        cluster_cols=None,
        labels=None,
    ):
        """
        Routine for creating a new BigQuery table.

        Args:
        - data (pandas.DataFrame): The data to insert into the new table.
        - schema (list of bigquery.SchemaField): Schema definition for the new table.
        - exists_ok (bool): If True, the table will be replaced if it already exists.
        - if_exists (str): If "replace", the table will be replaced if it already exists. If "append", the data will be appended to the table.
        - time_partition_col (str): The column to partition the table by. This is used for time-based partitioning.
        - range_partition_col (str): The column to partition the table by. This is used for range-based partitioning.
        - cluster_cols (list): The columns to cluster the table by.
        - labels (dict): Labels to apply to the table.

        Returns:
        - True if successful.
        """
        if time_partition_col and range_partition_col:
            raise ValueError(
                "Cannot have both time and range partitioning in the same table."
            )
        if isinstance(schema, dict):
            schema = dict_to_bigquery_fields(schema)

        if is_dataframe(data):
            try_import("pandas_gbq", "BigQuery.create_table.dataframe")
            from pandas_gbq import to_gbq

            if if_exists is None:
                if_exists = "replace" if exists_ok else "fail"
            return to_gbq(
                data,
                destination_table=self.table_id,
                if_exists=if_exists,
                location=self.location,
            )
        table = self.bigquery.Table(self.table_id, schema=schema)
        if time_partition_col:
            table.time_partitioning = self.bigquery.TimePartitioning(
                field=time_partition_col
            )
        if range_partition_col:
            table.range_partitioning = self.bigquery.RangePartitioning(
                field=range_partition_col
            )
        if cluster_cols:
            table.clustering_fields = cluster_cols
        if labels:
            table.labels = labels

        table = self.client.create_table(table, exists_ok=exists_ok)
        log(f"BigQuery - Table created: {self.table_id}")

        if data is not None:
            self.insert(data, schema=schema)
        return True

    def create_table(self, data=None, schema=None, exists_ok=True):
        """
        Creates a new BigQuery table.

        Args:
        - schema (list of bigquery.SchemaField or dict): The schema definition for the new table.

        Returns:
        - The created bigquery.Table object.

        Examples:
        - schema = [bigquery.SchemaField("name", "STRING"), bigquery.SchemaField("age", "INTEGER")]
        - or schema = {"name": "STRING", "age": "INTEGER"}
        - BigQuery().create_table("new_dataset.new_table", schema)
        """
        try:
            return self._create_table(data=data, schema=schema, exists_ok=exists_ok)
        except self.exceptions.NotFound:
            # Dataset does not exist, so create it and try again
            self.create_dataset(exists_ok=exists_ok)
            return self._create_table(data=data, schema=schema, exists_ok=exists_ok)
        return False

    def _create_external_table(
        self, uri, source_format=None, schema=None, infer_uri=True, exists_ok=True
    ):
        """
        Creates an external table in BigQuery.

        Args:
        - uri (str): The URI of the external data source.
        - source_format (str): The format of the external data source.
        - schema (list of bigquery.SchemaField): The schema of the external table.

        Returns:
        - True if successful.

        Examples:
        - BigQuery("dataset.external_table").create_external_table("gs://bucket/file.parquet")
        """
        if infer_uri:
            uri, source_format, extra_metadata = self._infer_uri(uri, source_format)

        external_config = self.bigquery.ExternalConfig(source_format=source_format)
        external_config.source_uris = [uri]
        # if schema:
        # external_config.schema = schema
        external_config.autodetect = True
        partition_columns = extra_metadata.get("partition_columns", None)
        if source_format == "PARQUET" and partition_columns:
            source_uri_prefix = uri.replace("*", "")
            partition_options = self.bigquery.external_config.HivePartitioningOptions
            options = {
                "mode": "STRINGS",
                "sourceUriPrefix": source_uri_prefix,
            }
            external_config.hive_partitioning = partition_options.from_api_repr(options)

        table = self.bigquery.Table(self.table_id)
        table.external_data_configuration = external_config
        try:
            self.client.create_table(table, exists_ok=exists_ok)
        except self.exceptions.NotFound:
            # Dataset does not exist, so create it and try again
            self.create_dataset()
            self.client.create_table(table)
        log(f"BigQuery - External table created: {self.table_id}")
        return True

    def create_external_table(self, uri, schema=None, exists_ok=True):
        """
        Creates an external table in BigQuery.

        Args:
        - uri (str): The URI of the external data source.
        - source_format (str): The format of the external data source.
        - schema (list of bigquery.SchemaField): The schema of the external table.

        Returns:
        - True if successful.

        Examples:
        - BigQuery("dataset.external_table").create_external_table("gs://bucket/file.parquet")
        """
        try:
            return self._create_external_table(uri, schema=schema, exists_ok=exists_ok)
        except self.exceptions.NotFound:
            # Dataset does not exist, so create it and try again
            self.create_dataset(exists_ok=exists_ok)
            return self._create_external_table(uri, schema=schema, exists_ok=exists_ok)

    def _infer_uri(self, uri, source_format=None):
        """
        Helper method to infer the source format from the URI.

        Args:
        - uri (str): The URI of the external data source, or the path to the file.
        - source_format (str): The format of the external data source.

        Returns:
        - Tuple of URI and source format.
        """
        extra_metadata = {}
        if not uri.startswith("gs://"):
            uri = f"gs://{uri}"

        from gcp_pal.storage import Storage

        if uri.endswith(".parquet"):
            source_format = "PARQUET"
            # Check is the file is partitioned
            from gcp_pal.storage import Storage

            if Storage(uri).isdir():
                from gcp_pal.storage.parquet import _get_partition_cols

                extra_metadata["partition_columns"] = _get_partition_cols(uri)
                if uri.endswith("/"):
                    uri = uri[:-1]
                uri = f"{uri}*"
        elif uri.endswith(".csv"):
            source_format = "CSV"
        elif uri.endswith(".json"):
            source_format = "NEWLINE_DELIMITED_JSON"
        elif uri.endswith(".avro"):
            source_format = "AVRO"
        elif uri.endswith(".orc"):
            source_format = "ORC"
        return uri, source_format, extra_metadata

    def create_dataset(self, exists_ok=True):
        """
        Creates a new BigQuery dataset.

        Returns:
        - True if successful.
        """
        dataset = self.bigquery.Dataset(self.dataset_id)
        dataset = self.client.create_dataset(dataset, exists_ok=exists_ok)
        log(f"BigQuery - Dataset created: {self.dataset_id}")
        return True

    def create(self, data=None, schema=None, exists_ok=True, infer_schema=False):
        """
        Creates a new BigQuery dataset or table.

        Args:
        - data (pandas.DataFrame|str): If a DataFrame, creates a table with the DataFrame schema.
                                       If a string, considers it a URI for an external table.
        - schema (list of bigquery.SchemaField): The schema definition for the new table.
        - exists_ok (bool): If True, the table will be replaced if it already exists.

        Returns:
        - True if successful.
        """
        if not self.table:
            # Working with a dataset.
            return self.create_dataset(exists_ok=exists_ok)

        # Auto infer the schema if not provided.
        if not schema and infer_schema is True:
            schema = Schema(data, is_data=True).bigquery()

        if isinstance(data, str):
            # Working with a table and input is a URI.
            return self.create_external_table(data, schema=schema, exists_ok=exists_ok)
        if isinstance(data, (list, dict)) or is_dataframe(data) or data is None:
            # Working with a table and input is a DataFrame.
            return self.create_table(data=data, schema=schema, exists_ok=exists_ok)
        else:
            raise ValueError("Invalid input for creating a table.")

    def delete_table(self, errors="raise"):
        """
        Deletes a BigQuery table.

        Returns:
        - True if successful.
        """
        try:
            self.client.delete_table(self.table_id)
        except Exception as e:
            log(f"BigQuery - Error deleting table: {e}")
            if errors == "raise":
                raise e
            return False
        log(f"BigQuery - Table deleted: BigQuery/{self.table_id}")
        return True

    def delete_dataset(self, errors="raise"):
        """
        Deletes a BigQuery dataset.

        Returns:
        - True if successful.
        """
        dataset_id = f"{self.project}.{self.dataset}"
        try:
            self.client.delete_dataset(dataset_id, delete_contents=True)
        except Exception as e:
            log(f"BigQuery - Error deleting dataset: {e}")
            if errors == "raise":
                raise e
            return False
        log(f"BigQuery - Dataset deleted: BigQuery/{dataset_id}")
        return True

    def delete(self, errors="ignore"):
        """
        Deletes a BigQuery dataset or table.

        Args:
        - errors (str): If "raise", errors will be raised. If "ignore", errors will be ignored.

        Returns:
        - True if successful.
        """
        if self.table:
            return self.delete_table(errors=errors)
        else:
            return self.delete_dataset(errors=errors)

    def list_datasets(self):
        """
        Lists all datasets in the BigQuery project.

        Returns:
        - List of dataset IDs.
        """
        datasets = list(self.client.list_datasets())
        dataset_ids = [dataset.dataset_id for dataset in datasets]
        log("BigQuery - datasets listed.")
        return dataset_ids

    def list_tables(self, dataset=None):
        """
        Lists all tables in a specified dataset.

        Args:
        - dataset_id (str): The ID of the dataset.

        Returns:
        - List of table IDs within the specified dataset.
        """
        dataset = dataset or self.dataset
        tables = list(self.client.list_tables(dataset))
        table_ids = [table.table_id for table in tables]
        log(f"BigQuery - Tables listed for dataset: {dataset}")
        return table_ids

    def ls(self, dataset=None):
        """
        Lists all datasets in the project, or all tables in a specified dataset.

        Returns:
        - List of dataset IDs or table IDs.
        """
        if self.dataset or dataset:
            return self.list_tables(dataset=dataset)
        else:
            return self.list_datasets()

    def exists(self):
        """
        Checks if a dataset or table exists.

        Returns:
        - True if the dataset or table exists.
        """
        output = False
        if self.table:
            try:
                self.client.get_table(self.table_id)
                output = True
            except self.exceptions.NotFound:
                output = False
        else:
            try:
                self.client.get_dataset(self.dataset_id)
                output = True
            except self.exceptions.NotFound:
                output = False
        return output

    def create_snapshot(self, snapshot_name: str):
        """
        Creates a snapshot of a BigQuery table.

        Args:
        - snapshot_name (str): The name of the snapshot.

        Returns:
        - True if successful.
        """
        success = self.client.create_snapshot(snapshot_name, self.table_id)
        log(f"BigQuery - Snapshot created: {snapshot_name}")
        return success

    def get_table(self):
        """
        Get the BigQuery table object.

        Returns:
        - The bigquery.Table object.
        """
        return self.client.get_table(self.table_id)

    def get_dataset(self):
        """
        Get the BigQuery dataset object.

        Returns:
        - The bigquery.Dataset object.
        """
        return self.client.get_dataset(self.dataset_id)

    def get(self):
        """
        Get the BigQuery dataset or table object.

        Returns:
        - The bigquery.Dataset or bigquery.Table object.
        """
        if self.table:
            return self.get_table()
        else:
            return self.get_dataset()

    def schema(self, as_dict=False):
        """
        Get the schema of the BigQuery table.

        Args:
        - as_dict (bool): If True, returns the schema as a dictionary. The output will be a (nested) dictionary:
                            `{"name": "type", "name": {"name": "type"}}`.

        Returns:
        - tuple[bigquery.SchemaField]: The schema of the table (column definitions).
        """
        schema = self.get_table().schema
        if as_dict:
            schema = bigquery_fields_to_dict(schema)
        return schema

    def set_schema(self, schema):
        """
        Set the schema of the BigQuery table.

        Args:
        - schema (list of bigquery.SchemaField or dict): The schema to set.

        Returns:
        - The updated bigquery.Table object.
        """
        if isinstance(schema, dict):
            schema = dict_to_bigquery_fields(schema)
        table = self.get_table()
        table.schema = schema
        return self.client.update_table(table, ["schema"])


if __name__ == "__main__":
    data = {
        "name": "John",
        "age": 30,
        "city": "New York",
    }
    BigQuery("dataset1.table1").write(data)
    exit()


if __name__ == "__main__":
    import pandas as pd
    from gcp_pal import Storage

    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    pa_schema = Schema(data, is_data=True).pyarrow()
    bq_schema = Schema(data, is_data=True).bigquery()
    bucket_name = f"test_bucket_vita_123"
    file_name = f"gs://{bucket_name}/file.csv"

    Storage(bucket_name).create()

    data.to_csv(file_name, index=False)
    success[0] = Storage(file_name).exists()
    success[1] = not Storage(file_name).isdir()
    success[2] = Storage(file_name).isfile()

    dataset_name = f"test_dataset_123"
    table_name = f"test_ext_table123"
    table_id = f"{dataset_name}.{table_name}"
    BigQuery(table_id).create(file_name, schema=bq_schema)
    success[3] = BigQuery(table_id).exists()

    queried_df = BigQuery().read(file_name, schema=bq_schema, to_dataframe=True)

    queried_df = queried_df.sort_values("a").reset_index(drop=True)
    queried_df = queried_df[data.columns]

    print(queried_df)
    print(data)
    print(queried_df.dtypes)
    print(data.dtypes)
    exit()


if __name__ == "__main__":
    import pandas as pd
    from uuid import uuid4
    from gcp_pal.storage import Storage, Parquet

    success = {}
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    pa_schema = Schema(data, is_data=True).pyarrow()
    bq_schema = Schema(data, is_data=True).bigquery()
    bucket_name = f"test_bucket_{uuid4().hex}"
    file_name = f"gs://{bucket_name}/file.parquet"
    Storage(bucket_name).create()

    Parquet(file_name).write(data, partition_cols=["a"], schema=pa_schema)

    dataset_name = f"test_dataset_{uuid4().hex}"
    table_name = f"test_ext_table"
    table_id = f"{dataset_name}.{table_name}"
    BigQuery(table_id).create(file_name, schema=bq_schema)

    queried_df = BigQuery().read(file_name, schema=bq_schema)

    print(queried_df)
    print(data)
    exit()


if __name__ == "__main__":
    # Example usage
    from google.cloud import bigquery

    dataset = "clean"
    print(BigQuery().ls())
    print(BigQuery(dataset=dataset).get_dataset().dataset_id)
    BigQuery(f"{dataset}.new_table1").create_table(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
    )
    BigQuery(f"{dataset}.new_table1").insert(
        [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
    )
    df = BigQuery(f"{dataset}.new_table1").read(
        columns=["name", "age"],
        filters=[("age", ">", 25), ("name", "like", "J%")],
        to_dataframe=True,
    )

    print(df)

    print(BigQuery().ls())  # List all datasets
    print(BigQuery(dataset=f"{dataset}").ls())  # List all tables in the "clean" dataset
    BigQuery(dataset=f"{dataset}").delete()
