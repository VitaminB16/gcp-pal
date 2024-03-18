from __future__ import annotations

import os
from google.cloud import bigquery
from google.auth import default as google_auth_default
from google.api_core.exceptions import NotFound as NotFoundError

from gcp_tools.utils import log, enforce_schema, is_dataframe


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
        if "." in self.dataset:
            # E.g. BigQuery(dataset="project.dataset")
            self.project, self.dataset = self.dataset.split(".", 1)
        self.client = bigquery.Client(project=self.project)

    def query(self, sql, job_config=None, to_dataframe=True):
        """
        Executes a query against BigQuery.

        Args:
        - sql (str): SQL query string.
        - job_config (bigquery.QueryJobConfig): Optional configuration for the query job.
        - to_dataframe (bool): If True, returns the results as a pandas DataFrame.

        Returns:
        - Query results as a pandas DataFrame or BigQuery job object.
        """
        output = self.client.query(sql, job_config=job_config)
        log(f"Query executed: {sql}")
        if to_dataframe:
            output = output.to_dataframe()
        return output

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
            return data.to_gbq(table, if_exists="append")

        if not schema and isinstance(data, list):
            table = self.client.get_table(self.table)  # Make sure the table exists
            errors = self.client.insert_rows_json(table, data)
        elif schema:
            table = bigquery.Table(self.table, schema=schema)
            table = self.client.create_table(table, exists_ok=True)
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
            return to_gbq(
                data,
                f"{self.dataset}.{self.table}",
                project_id=self.project,
                if_exists=if_exists,
            )

        table_id = f"{self.project}.{self.dataset}.{self.table}"
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table, exists_ok=exists_ok)
        log(f"Table created: {table_id}")

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
        dataset_id = f"{self.project}.{self.dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset = self.client.create_dataset(dataset, exists_ok=exists_ok)
        log(f"Dataset created: {dataset_id}")
        return True

    def create(self, data=None, schema=None, exists_ok=True):
        """
        Creates a new BigQuery dataset or table.

        Returns:
        - True if successful.
        """
        if self.table:
            return self.create_table(data, schema, exists_ok=exists_ok)
        else:
            return self.create_dataset(exists_ok=exists_ok)

    def delete_table(self, errors="raise"):
        """
        Deletes a BigQuery table.

        Returns:
        - True if successful.
        """
        table_id = f"{self.project}.{self.dataset}.{self.table}"
        try:
            self.client.delete_table(table_id)
        except Exception as e:
            log(f"Error deleting table: {e}")
            if errors == "raise":
                raise e
            return False
        log(f"Table deleted: {table_id}")
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
    print(BigQuery(dataset="clean").ls())
    BigQuery("clean2.new_table").create_table(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
    )
    exit()
    print(BigQuery(dataset="clean").ls())
    BigQuery("clean2.new_table").delete()
    print(BigQuery(dataset="clean").ls())
