from __future__ import annotations

import os
import pandas as pd
from google.auth import default as google_auth_default
from google.cloud import bigquery
from gcp_tools.utils import log, enforce_schema


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
            self.project, self.dataset, self.table = table.split(".")
        except (ValueError, AttributeError):
            pass
        try:
            self.dataset, self.table = self.table.split(".")
        except (ValueError, AttributeError):
            pass
        self.client = bigquery.Client(project=self.project)
        print(f"Table: {self.table}")
        print(f"Dataset: {self.dataset}")
        print(f"Project: {self.project}")

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
