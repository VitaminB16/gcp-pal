import json

from gcp_pal.utils import (
    log,
    ClientHandler,
    ModuleHandler,
    get_default_arg,
    get_all_kwargs,
)


class PubSub:
    """
    Class for pushing a payload to a topic

    Examples:
    - `PubSub("topic").publish("data")` -> Publish `"data"` to "topic"
    - `PubSub("topic").publish({"key": "value"})` -> Publish `{"key": "value"}` to "topic"
    """

    def __init__(
        self,
        path: str = "",
        topic: str = None,
        subscription: str = None,
        project=None,
    ):
        """
        Args:
        - `path` (str): Path to the topic or project. Default is "". Supported formats:
            - "projects/my-project/topics/my-topic/subscription/my-subscription"
            - "projects/my-project/topics/my-topic"
            - "projects/my-project"
            - "my-project/my-topic/my-subscription"
            - "my-project/my-topic"
            - "my-project"
        - `topic` (str): Name of the topic
        - `subscription` (str): Name of the subscription
        - `project` (str): Project ID
        """
        self.subscription = None
        self.topic_id = None
        self.project = None
        self.path = path
        if path.startswith("projects/"):
            path = path.split("/")[1::2]
            path = "/".join(path)
            self.path = path
        try:
            self.project, self.topic_id, self.subscription = path.split("/")
        except ValueError:
            pass
        try:
            self.project, self.topic_id = path.split("/")
        except ValueError:
            pass
        if isinstance(path, str) and path.count("/") == 0:
            self.project = path
        self.topic_id = self.topic_id or topic
        self.subscription = self.subscription or subscription
        self.project = self.project or project or get_default_arg("project")
        self.level = self._set_level()
        self.path = self._set_path()
        self.parent = f"projects/{self.project}"
        if self.level == "topic":
            self.parent = f"{self.parent}/topics/{self.topic_id}"

        self.pubsub = ModuleHandler("google.cloud").please_import(
            "pubsub_v1", who_is_calling="PubSub"
        )
        self.publisher = ClientHandler(self.pubsub.PublisherClient).get()
        self.subscriber = ClientHandler(self.pubsub.SubscriberClient).get()
        self.types = self.pubsub.types
        self.exceptions = ModuleHandler("google.api_core").please_import(
            "exceptions", who_is_calling="PubSub"
        )
        self.topic_path = self.publisher.topic_path(self.project, self.topic_id)
        self.duration = ModuleHandler("google.protobuf").please_import(
            "duration_pb2", who_is_calling="PubSub"
        )

    def __repr__(self):
        return f"PubSub({self.topic_id})"

    def _set_level(self):
        if self.subscription and self.project:
            return "subscription"
        elif self.topic_id and self.project:
            return "topic"
        elif self.project:
            return "project"
        else:
            raise ValueError("Invalid level.")

    def _set_path(self):
        path = self.project
        if self.topic_id:
            path = f"{path}/{self.topic_id}"
        if self.subscription:
            path = f"{path}/{self.subscription}"
        return path

    def ls_topics(self, full_name=False, include_project=False):
        """
        Lists all topics in a project.

        Args:
        - `full_name` (bool): If False, returns names in format "{project}/{topic}".
                              Otherwise, returns full names "projects/{project}/topics/{topic}".
                              Default is False.
        - `include_project` (bool): If False, returns names in format "{topic}".
        """
        topics = self.publisher.list_topics(request={"project": self.parent})
        output = [topic.name for topic in topics]
        if not full_name:
            output = ["/".join(topic.split("/")[1::2]) for topic in output]
            if not include_project:
                output = ["/".join(topic.split("/")[1:]) for topic in output]
        return output

    def ls_subscriptions(self, full_name=False, include_project=False):
        """
        Lists all subscriptions for a topic.

        Args:
        - `full_name` (bool): If False, returns names in format "{project}/{topic}/{subscription}".
                              Otherwise, returns full names "projects/{project}/topics/{topic}/subscriptions/{subscription}".
                              Default is False.
        - `include_project` (bool): If False, returns names in format "{topic}/{subscription}".
        """
        if self.level == "topic":
            subscriptions = self.publisher.list_topic_subscriptions(
                request={"topic": self.parent}
            )
        elif self.level == "project":
            subscriptions = self.subscriber.list_subscriptions(
                request={"project": self.parent}
            )
            subscriptions = [subscription.name for subscription in subscriptions]
        output = [subscription for subscription in subscriptions]
        if not full_name:
            output = [
                "/".join(subscription.split("/")[1::2]) for subscription in output
            ]
            if not include_project:
                output = [
                    "/".join(subscription.split("/")[1:]) for subscription in output
                ]
        return output

    def ls(self, full_name=False, include_project=False):
        """
        Lists all topics or subscriptions.

        Args:
        - `full_name` (bool): If False, returns names in format "{project}/{topic}" or "{topic}/{subscription}".
                              Otherwise, returns full names "projects/{project}/topics/{topic}" or "projects/{project}/topics/{topic}/subscriptions/{subscription}".
                              Default is False.
        - `include_project` (bool): If False, returns names in format "{topic}" or "{subscription}".
        """
        if self.level == "topic":
            return self.ls_subscriptions(
                full_name=full_name, include_project=include_project
            )
        elif self.level == "project":
            return self.ls_topics(full_name=full_name, include_project=include_project)
        else:
            raise ValueError(f"Invalid level: {self.level}")

    def publish(self, data) -> None:
        if isinstance(data, dict):
            data = json.dumps(data)
        try:
            publish_future = self.publisher.publish(self.parent, data.encode("utf-8"))
            result = publish_future.result()
            log(
                f"PubSub - Published message: {result} to {self.topic_id} in {self.project}."
            )
            return result
        except Exception as e:
            log(f"PubSub - An error occurred: {e}")
            return

    def create_topic(
        self,
        topic=None,
        labels=None,
        schema=None,
        encoding=None,
        message_retention_duration=None,
        if_exists="ignore",
    ):
        """
        Create a topic.

        Args:
        - `topic` (str): Name of the topic. Can be specified in the constructor.
        - `labels` (dict): Labels for the topic. Default is None.
        - `schema` (str): Name of the schema against which to validate message payloads. Has the form `projects/{project}/schemas/{schema}`.
        - `encoding` (str): The encoding of the messages validated against the schema. Default is None.
        - `message_retention_duration` (int): The duration (in seconds) for which the topic retains unacknowledged messages. Default is None.
        """
        topic = topic or self.topic_id
        schema_settings = (
            self.types.SchemaSettings(schema=schema, encoding=encoding)
            if schema
            else None
        )
        if message_retention_duration:
            message_retention_duration = self.duration.Duration(
                seconds=message_retention_duration
            )
        log(f"PubSub - Creating topic: {self.topic_id} in {self.project}...")
        topic_resource = self.types.Topic(
            name=self.parent,
            schema_settings=schema_settings,
            labels=labels,
        )
        try:
            result = self.publisher.create_topic(request=topic_resource)
        except self.exceptions.AlreadyExists as e:
            if if_exists != "ignore":
                raise e
            log(f"PubSub - Topic already exists: {self.topic_id} in {self.project}")
            result = self.publisher.get_topic(request={"topic": self.parent})
            return result
        log(f"PubSub - Topic created: {result.name} in {self.project}")
        return result

    def create_subscription(
        self,
        subscription=None,
        topic=None,
        ack_deadline_seconds=None,
        retain_acked_messages=None,
        message_retention_duration=None,
        labels=None,
        minimum_backoff=10,
        maximum_backoff=600,
        detached=False,
        filter=None,
        table_id=None,
        if_exists="ignore",
        **kwargs,
    ):
        """
        Create a subscription.

        Args:
        - `subscription` (str): Name of the subscription. Can be specified in the constructor.
        - `topic` (str): Name of the topic. Can be specified in the constructor.
        - `ack_deadline_seconds` (int): The maximum time after receiving a message that the subscriber must acknowledge the message. Default is None.
        - `retain_acked_messages` (bool): Whether to retain acknowledged messages. Default is None.
        - `message_retention_duration` (int): How long to retain unacknowledged messages. Default is None.
        - `labels` (dict): Labels for the subscription. Default is None.
        - `minimum_backoff` (int): Minimum delay between redelivery attempts. Default is 10.
        - `maximum_backoff` (int): Maximum delay between redelivery attempts. Default is 600.
        - `detached` (bool): If True, the subscription will be detached from the topic. Default is False.
        - `filter` (str): A filter expression that determines which messages are delivered to the subscription. Default is None.
        - `table_id` (str): The BigQuery table ID to which the subscription's data will be written. Default is None.
        - `if_exists` (str): If "ignore", does not raise an error if the subscription already exists. Default is "ignore".
        - `kwargs`: Additional arguments to be passed to the relevant configurations.

        Returns:
        - The reference to the created subscription.
        """
        log(f"PubSub - Creating subscription: {self.subscription} in {self.project}...")
        subscription = subscription or self.subscription
        topic = topic or self.topic_id
        name = f"projects/{self.project}/subscriptions/{subscription}"
        retry_policy = self.types.RetryPolicy(
            minimum_backoff=self.duration.Duration(seconds=minimum_backoff),
            maximum_backoff=self.duration.Duration(seconds=maximum_backoff),
        )
        if message_retention_duration:
            duration_pb2 = ModuleHandler("google.protobuf").please_import(
                "duration_pb2", who_is_calling="PubSub"
            )
            message_retention_duration = duration_pb2.Duration(
                seconds=message_retention_duration
            )
        table = table_id
        all_kwargs = get_all_kwargs(locals())
        bigquery_config, storage_config, push_config = self._split_subscription_kwargs(
            all_kwargs
        )
        bigquery_config = (
            self.types.BigQueryConfig(**bigquery_config) if bigquery_config else None
        )
        storage_config = (
            self.types.CloudStorageConfig(**storage_config) if storage_config else None
        )
        push_config = self.types.PushConfig(**push_config) if push_config else None
        subscription_resource = self.types.Subscription(
            name=name,
            topic=f"projects/{self.project}/topics/{topic}",
            ack_deadline_seconds=ack_deadline_seconds,
            retain_acked_messages=retain_acked_messages,
            message_retention_duration=message_retention_duration,
            labels=labels,
            retry_policy=retry_policy,
            detached=detached,
            filter=filter,
            bigquery_config=bigquery_config,
            cloud_storage_config=storage_config,
            push_config=push_config,
        )
        try:
            result = self.subscriber.create_subscription(request=subscription_resource)
        except self.exceptions.AlreadyExists as e:
            if if_exists != "ignore":
                raise e
            log(
                f"PubSub - Subscription already exists: {self.subscription} in {self.project}"
            )
            result = self.subscriber.get_subscription(request={"subscription": name})
            return result
        log(f"PubSub - Subscription created: {result.name} in {self.project}.")
        return subscription

    def _split_subscription_kwargs(self, kwargs):
        """
        Splits the subscription kwargs into function, service and build kwargs.

        Returns:
        - (dict, dict, dict) The function, service and build kwargs.
        """
        bigquery_kwargs = {}
        cloud_storage_kwargs = {}
        push_kwargs = {}
        for key, value in kwargs.items():
            if value is None:
                continue
            if key in self.types.BigQueryConfig.__annotations__:
                bigquery_kwargs[key] = value
            if key in self.types.PushConfig.__annotations__:
                push_kwargs[key] = value
            if key in self.types.CloudStorageConfig.__annotations__:
                cloud_storage_kwargs[key] = value
        return bigquery_kwargs, cloud_storage_kwargs, push_kwargs

    def delete_topic(self, errors="ignore"):
        """
        Delete a topic.

        Args:
        - `errors` (str): If "ignore", does not raise an error if the topic does not exist. Default is "ignore".

        Returns:
        - True if the topic no longer exists.
        """
        log(f"PubSub - Deleting topic: {self.topic_id} in {self.project}...")
        try:
            self.publisher.delete_topic(request={"topic": self.topic_path})
        except self.exceptions.NotFound as e:
            if errors != "ignore":
                raise e
            log(
                f"PubSub - Topic not found to delete: {self.topic_id} in {self.project}"
            )
            return
        log(f"PubSub - Topic deleted: {self.topic_id} in {self.project}")
        return True

    def delete_subscription(self, errors="ignore"):
        """
        Delete a subscription.

        Args:
        - `errors` (str): If "ignore", does not raise an error if the subscription does not exist. Default is "ignore".

        Returns:
        - True if the subscription no longer exists.
        """
        parent = f"projects/{self.project}/subscriptions/{self.subscription}"
        log(f"PubSub - Deleting subscription: {self.subscription} in {self.project}...")
        try:
            self.subscriber.delete_subscription(request={"subscription": parent})
        except self.exceptions.NotFound as e:
            if errors != "ignore":
                raise e
            log(
                f"PubSub - Subscription not found to delete: {self.subscription} in {self.project}"
            )
            return
        log(f"PubSub - Subscription deleted: {self.subscription} in {self.project}")
        return True

    def delete(self, errors="ignore"):
        """
        Delete a topic or subscription.

        Args:
        - `errors` (str): If "ignore", does not raise an error if the topic or subscription does not exist. Default is "ignore".
        """
        if self.level == "topic":
            return self.delete_topic(errors=errors)
        elif self.level == "subscription":
            return self.delete_subscription(errors=errors)
        else:
            raise ValueError(f"Invalid level: {self.level}")

    def create(self, **kwargs):
        """
        Create a topic or subscription.

        Args:
        - `kwargs`: Keyword arguments to be passed to the relevant `create()` method.

        Returns:
        - The topic or subscription.
        """
        if self.level == "topic":
            return self.create_topic(**kwargs)
        elif self.level == "subscription":
            return self.create_subscription(**kwargs)
        else:
            raise ValueError(f"Invalid level: {self.level}")

    def get(self, level=None):
        """
        Get a reference to the topic or subscription.

        Args:
        - `level` (str): Level to get. Either "topic" or "subscription". Default is `self.level`.

        Returns:
        - The topic or subscription.
        """
        level = level or self.level
        if level == "topic":
            return self.publisher.get_topic(request={"topic": self.parent})
        elif level == "subscription":
            parent = f"projects/{self.project}/subscriptions/{self.subscription}"
            return self.subscriber.get_subscription(request={"subscription": parent})
        else:
            raise ValueError(f"Invalid level: {level}")

    def exists(self):
        try:
            self.get()
            return True
        except self.exceptions.NotFound:
            return False


if __name__ == "__main__":
    PubSub().ls()
    PubSub().ls_subscriptions()
    # PubSub("test_topic").publish("data")
    # PubSub("test_topic").publish({"key": "value"})
