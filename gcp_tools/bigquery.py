from __future__ import annotations

import os
from google.cloud import bigquery
from google.auth import default as google_auth_default
from google.api_core.exceptions import NotFound as NotFoundError

from gcp_tools.utils import log, enforce_schema, is_dataframe


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
        self.query_parts.append(f"LIMIT {limit}")
        return self

    def _where(self, filters: list[tuple]) -> tuple:
        """
        Helper to build a WHERE clause safely.
        """
        conditions = []
        params = {}
        for i, (col, op, value) in enumerate(filters):
            self._check_illegal_filters(filters)
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

    def _check_illegal_filters(self, filters):
        """
        Helper to check for illegal characters in filters.
        """
        for col, op, value in filters:
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
        return filters


class BigQuery:
    """
    Class for operating Google BigQuery.
    """

    def __init__(self, table=None, dataset=None, project=None):
        """
        Initializes the BigQuery client.

        Args:
        - project (str): Project ID for the BigQuery service.

        Examples:
        - BigQuery().query("SELECT * FROM `project.dataset.table`") -> Executes a query and returns the results.
        - BigQuery("dataset.table").insert(data) -> Inserts data into the specified BigQuery table.
        - BigQuery("dataset.new_table").create_table(schema=schema) -> Creates a new table with the specified schema.
        """
        self.table = table
        self.dataset = dataset
        self.project = project or os.environ.get("PROJECT") or google_auth_default()[1]
        try:
            # E.g. BigQuery("project.dataset.table")
            self.project, self.dataset, self.table = table.split(".")
        except (ValueError, AttributeError):
            pass
        try:
            # E.g. BigQuery("dataset.table", project="project")
            self.dataset, self.table = self.table.split(".")
        except (ValueError, AttributeError):
            pass
        if self.dataset and "." in self.dataset:
            # E.g. BigQuery(dataset="project.dataset")
            self.project, self.dataset = self.dataset.split(".", 1)

        # table_id is the full table ID, e.g. "project.dataset.table"
        self.table_id = f"{self.dataset}.{self.table}"
        if self.project:
            self.table_id = f"{self.project}.{self.table_id}"
        if self.project and self.dataset:
            self.dataset_id = f"{self.project}.{self.dataset}"
        self.client = bigquery.Client(project=self.project)

    def query(self, sql, job_config=None, to_dataframe=True, params=None):
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
            job_config = self._parse_query_params(params)
        output = self.client.query(sql, job_config=job_config)
        log(f"Query executed: {sql}")
        if to_dataframe:
            output = output.to_dataframe()
        return output

    def read(
        self,
        job_config=None,
        to_dataframe=True,
        columns=None,
        filters=None,
        schema=None,
    ):
        """
        Reads entire table from BigQuery.
        """
        sql, params = SQLBuilder(self.table_id).select(columns).where(filters).build()
        return self.query(
            sql=sql,
            job_config=job_config,
            to_dataframe=to_dataframe,
            params=params,
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
            job_config = bigquery.QueryJobConfig()

        query_params = []

        for key, value in params.items():
            val_type = {"int": "INTEGER", "float": "FLOAT", "str": "STRING"}.get(
                type(value).__name__, "STRING"
            )
            if isinstance(value, list):
                query_params.append(bigquery.ArrayQueryParameter(key, val_type, value))
            else:
                query_params.append(bigquery.ScalarQueryParameter(key, val_type, value))
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
            from pandas_gbq import to_gbq

            return to_gbq(data, destination_table=self.table_id, if_exists="append")

        if not schema and isinstance(data, list):
            table = self.client.get_table(self.table_id)  # Make sure the table exists
            errors = self.client.insert_rows_json(table, data)
        elif schema:
            table = bigquery.Table(self.table_id, schema=schema)
            errors = self.client.insert_rows_json(table, data)
        else:
            raise ValueError(
                "Data format not supported or schema required for new table."
            )

        if errors == []:
            log(f"Data inserted into {self.table}")
            return True
        else:
            log(f"Errors occurred while inserting data into {self.table}: {errors}")
            return False

    def _create_table(self, schema=None, data=None, exists_ok=True):
        """
        Routine for creating a new BigQuery table.

        Args:
        - data (pandas.DataFrame): The data to insert into the new table.
        - schema (list of bigquery.SchemaField): Schema definition for the new table.
        - exists_ok (bool): If True, the table will be replaced if it already exists.

        """
        if is_dataframe(data):
            from pandas_gbq import to_gbq

            if_exists = "replace" if exists_ok else "fail"
            return to_gbq(data, destination_table=self.table_id, if_exists=if_exists)

        table = bigquery.Table(self.table_id, schema=schema)
        table = self.client.create_table(table, exists_ok=exists_ok)
        log(f"Table created: {self.table_id}")

        return True

    def create_table(self, data=None, schema=None, exists_ok=True):
        """
        Creates a new BigQuery table.

        Args:
        - schema (list of bigquery.SchemaField): Schema definition for the new table.

        Returns:
        - The created bigquery.Table object.

        Examples:
        - schema = [bigquery.SchemaField("name", "STRING"), bigquery.SchemaField("age", "INTEGER")]
        - BigQuery().create_table("new_dataset.new_table", schema)
        """
        try:
            return self._create_table(data=data, schema=schema, exists_ok=exists_ok)
        except NotFoundError:
            # Dataset does not exist, so create it and try again
            self.create_dataset(exists_ok=exists_ok)
            return self._create_table(data=data, schema=schema, exists_ok=exists_ok)
        return False

    def create_dataset(self, exists_ok=True):
        """
        Creates a new BigQuery dataset.

        Returns:
        - True if successful.
        """
        dataset = bigquery.Dataset(self.dataset_id)
        dataset = self.client.create_dataset(dataset, exists_ok=exists_ok)
        log(f"Dataset created: {self.dataset_id}")
        return True

    def create(self, data=None, schema=None, exists_ok=True):
        """
        Creates a new BigQuery dataset or table.

        Returns:
        - True if successful.
        """
        if self.table:
            return self.create_table(data=data, schema=schema, exists_ok=exists_ok)
        else:
            return self.create_dataset(exists_ok=exists_ok)

    def delete_table(self, errors="raise"):
        """
        Deletes a BigQuery table.

        Returns:
        - True if successful.
        """
        try:
            self.client.delete_table(self.table_id)
        except Exception as e:
            log(f"Error deleting table: {e}")
            if errors == "raise":
                raise e
            return False
        log(f"Table deleted: {self.table_id}")
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
            log(f"Error deleting dataset: {e}")
            if errors == "raise":
                raise e
            return False
        log(f"Dataset deleted: {dataset_id}")
        return True

    def delete(self, errors="raise"):
        """
        Deletes a BigQuery dataset or table.

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
        log("Datasets listed.")
        return dataset_ids

    def list_tables(self):
        """
        Lists all tables in a specified dataset.

        Args:
        - dataset_id (str): The ID of the dataset.

        Returns:
        - List of table IDs within the specified dataset.
        """
        tables = list(self.client.list_tables(self.dataset))
        table_ids = [table.table_id for table in tables]
        log(f"Tables listed for dataset: {self.dataset}")
        return table_ids

    def ls(self):
        """
        Lists all datasets in the project, or all tables in a specified dataset.

        Returns:
        - List of dataset IDs or table IDs.
        """
        if self.dataset:
            return self.list_tables()
        else:
            return self.list_datasets()


if __name__ == "__main__":
    # Example usage
    BigQuery("clean2.new_table").create_table(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
    )
    BigQuery("clean2.new_table").insert(
        [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
    )
    df = BigQuery("clean2.new_table").read(
        columns=["name", "age"],
        filters=[("age", ">", 25), ("name", "like", "J%")],
        to_dataframe=True,
    )
    print(df)

    print(BigQuery().ls())  # List all datasets
    print(BigQuery(dataset="clean").ls())  # List all tables in the "clean" dataset
    BigQuery(dataset="clean2").delete()
