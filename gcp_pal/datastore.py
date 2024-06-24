from gcp_pal.utils import ModuleHandler, ClientHandler, get_default_arg, log


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

        self.datastore = ModuleHandler("google.cloud").please_import(
            "datastore", who_is_calling="Datastore"
        )

        self.client = ClientHandler(self.datastore.Client).get(
            project=self.project,
            namespace=self.namespace,
            database=self.database,
        )
        self.admin = ModuleHandler("google.cloud").please_import(
            "firestore_admin_v1", who_is_calling="Firestore"
        )
        self.admin_client = ClientHandler(self.admin.FirestoreAdminClient).get()

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
        paths = {
            "project": self.project,
            "database": self.database,
            "namespace": self.namespace,
            "kind": self.kind,
            "entity": self.entity,
        }
        paths = [f"{key}={value}" for key, value in paths.items() if value]
        output = f"Datastore({', '.join(paths)})"
        return output

    def query(self, filters: dict = None, order: str = None, limit: int = None):
        """
        Returns a query object to interact with the Datastore.

        Returns:
        - Query: Query object to interact with the Datastore.
        """
        query = self.client.query(kind=self.kind, namespace=self.namespace)

        if filters:
            for key, value in filters.items():
                query.add_filter(key, "=", value)

        if order:
            query.order = order

        if limit:
            query.limit = limit

        return query

    def fetch(
        self,
        filters: dict = None,
        order: str = None,
        limit: int = None,
        as_dict: bool = False,
    ):
        """
        Fetches the entities from the Datastore.

        Args:
        - filters (dict): Filters to be applied to the query. Default is None.
        - order (str): Order of the entities. Default is None.
        - limit (int): Limit of the entities. Default is None.

        Returns:
        - list: List of entities fetched from the Datastore.
        """
        if not self.namespace:
            self.namespace = "default"
        if not self.database:
            self.database = "default"
        log(f"Datastore - Fetching {self}...")

        query = self.query(filters=filters, order=order, limit=limit)
        if as_dict:
            entities = [dict(entity) for entity in query.fetch()]
        else:
            entities = list(query.fetch())
        return entities

    def fetch_one(self, filters: dict = None, order: str = None, as_dict: bool = False):
        """
        Fetches one entity from the Datastore.

        Args:
        - filters (dict): Filters to be applied to the query. Default is None.
        - order (str): Order of the entities. Default is None.

        Returns:
        - Entity: Entity fetched from the Datastore.
        """
        entities = self.fetch(filters=filters, order=order, limit=1, as_dict=as_dict)
        if entities:
            return entities[0]
        return None

    def read(self, **kwargs):
        """
        Alias for fetch method.

        Args:
        - kwargs: Keyword arguments to be passed to the fetch method.

        Returns:
        - Entity: Entity fetched from the Datastore.
        """
        return self.fetch(**kwargs)

    def ls_databases(self, full_path=False):
        """
        List all databases in a Firestore project.
        """
        databases = self.admin_client.list_databases(parent=f"projects/{self.project}")
        output = [database.name for database in databases.databases]
        if not full_path:
            output = [database.split("/")[-1] for database in output]
        log(f"Firestore - databases listed.")
        return output


if __name__ == "__main__":
    ds = Datastore(database="database1")
    print(ds)
    breakpoint()
