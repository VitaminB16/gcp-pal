import os
from gcp_pal.utils import try_import

try_import("google.cloud.datastore", "Datastore")
from google.cloud import datastore

from gcp_pal.utils import get_auth_default, ClientHandler, get_default_arg


class Datastore:

    _clients = {}

    def __init__(
        self,
        namespace: str = None,
        kind: str = None,
        entity: dict = None,
        database: str = None,
        project: str = None,
        location: str = "europe-west2",
    ):
        """
        Datastore class to interact with Google Cloud Datastore.

        Args:
        - database (str): Database of the entity. Default is None.
        - namespace (str): Namespace of the entity. Default is None.
        - kind (str): Kind of the entity. Default is None.
        - entity (dict): Entity to be stored. Default is None.
        - project (str): Project of the entity. Default is "PROJECT" environment variable or the gcloud default project.
        - location (str): Location of the entity. Default is "europe-west2".
        """
        self.project = project or get_default_arg("project")
        self.database = database
        self.namespace = namespace
        self.kind = kind
        self.entity = entity
        self.location = location

        self._set_up_attributes()

        self.client = ClientHandler(datastore.Client).get(
            project=self.project,
            namespace=self.namespace,
            database=self.database,
        )

    def _set_up_attributes(self):
        """
        Sets up the attributes of the Datastore object. Extracts the project, database, namespace, kind and entity from the path.
        """
        self.level = self._get_level()
        self.path = self._get_path()

    def _get_level(self):
        """
        Returns the level of the path. It can be one of: project, database, namespace, kind, entity.

        Returns:
        - str: Level of the path.
        """
        if self.entity:
            if not self.namespace:
                self.namespace = "default"
            if not self.database:
                self.database = "default"
            return "entity"
        elif self.kind:
            if not self.namespace:
                self.namespace = "default"
            if not self.database:
                self.database = "default"
            return "kind"
        elif self.namespace:
            if not self.database:
                self.database = "default"
            return "namespace"
        elif self.database:
            return "database"
        else:
            return "project"

    def _get_path(self):
        """
        Returns the path of the entity in the format: "project.database.namespace/kind/entity".

        Returns:
        - str: Path of the entity.
        """
        if self.entity:
            return f"{self.project}.{self.database}/{self.namespace}/{self.kind}/{self.entity}"
        elif self.kind:
            return f"{self.project}.{self.database}/{self.namespace}/{self.kind}"
        elif self.namespace:
            return f"{self.project}.{self.database}/{self.namespace}"
        elif self.database:
            return f"{self.project}.{self.database}"
        else:
            return f"{self.project}"

    def __repr__(self):
        return f"Datastore({self.level} = {self.path})"

    def query(self, filters: dict = None, order: str = None, limit: int = None):
        """
        Returns a query object to interact with the Datastore.

        Returns:
        - Query: Query object to interact with the Datastore.
        """
        return self.client.query(kind=self.kind, namespace=self.namespace)


if __name__ == "__main__":
    ds = Datastore("namespace")
    print(ds)
