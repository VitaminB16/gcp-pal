class ClientHandler:
    """
    A class for handling clients. If the client is already created, return it. Otherwise, create it.
    """

    _clients = {}

    def __init__(self, client_initializer):
        """
        Initialize the ClientHandler.

        Args:
        - client_initializer: A function that initializes the client. E.g. `google.cloud.bigquery.Client`.
        """
        self.client_initializer = client_initializer
        self.module_name = client_initializer.__module__
        try:
            self.client_name = client_initializer.__name__
        except AttributeError:
            self.client_name = client_initializer.__class__.__name__
        self.initializer_name = f"{self.module_name}.{self.client_name}"

    def get(self, force_refresh=False, **kwargs):
        """
        Get the client. If the client is already created, return it. Otherwise, create it.

        Args:
        - force_refresh: If True, create a new client even if the client is already created.
        - kwargs: The arguments to pass to the client_initializer. E.g. `project="my-project"`, `location="europe-west2"`.

        Returns:
        - client: The client.
        """
        input_key = frozenset(kwargs.items())
        client_key = (self.initializer_name, input_key)
        if client_key in ClientHandler._clients and not force_refresh:
            client = ClientHandler._clients[client_key]
        else:
            client = self.client_initializer(**kwargs)
            ClientHandler._clients[client_key] = client
        return client


if __name__ == "__main__":
    from google.cloud import bigquery, firestore
    import time

    time_now = time.time()
    bq_client1 = ClientHandler(bigquery.Client).get()
    time_after_bq_client1 = time.time()
    print(f"Time taken for bq_client1: {time_after_bq_client1 - time_now}")
    bq_client2 = ClientHandler(bigquery.Client).get()
    time_after_bq_client2 = time.time()
    print(f"Time taken for bq_client2: {time_after_bq_client2 - time_after_bq_client1}")
    bq_client3 = ClientHandler(bigquery.Client).get(force_refresh=True)
    time_after_bq_client3 = time.time()
    print(f"Time taken for bq_client3: {time_after_bq_client3 - time_after_bq_client2}")

    fs_client1 = ClientHandler(firestore.Client).get()
    time_after_fs_client1 = time.time()
    print(f"Time taken for fs_client1: {time_after_fs_client1 - time_after_bq_client3}")
    fs_client2 = ClientHandler(firestore.Client).get(project="my-project")
    time_after_fs_client2 = time.time()
    print(f"Time taken for fs_client2: {time_after_fs_client2 - time_after_fs_client1}")
    fs_client3 = ClientHandler(firestore.Client).get(force_refresh=True)
    time_after_fs_client3 = time.time()
    print(f"Time taken for fs_client3: {time_after_fs_client3 - time_after_fs_client2}")
    fs_client4 = ClientHandler(firestore.Client).get(project="my-project")
    time_after_fs_client4 = time.time()
    print(f"Time taken for fs_client4: {time_after_fs_client4 - time_after_fs_client3}")

    assert bq_client1 is bq_client2
    assert bq_client1 is not bq_client3

    assert fs_client1 is not fs_client2
    assert fs_client1 is not fs_client3
    assert fs_client2 is fs_client4

    assert len(ClientHandler._clients) == 3
