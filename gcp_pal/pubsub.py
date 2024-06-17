import json

from gcp_pal.utils import log, ClientHandler, ModuleHandler, get_default_arg


class PubSub:
    """
    Class for pushing a payload to a topic

    Examples:
    - `PubSub("topic").publish("data")` -> Publish `"data"` to "topic"
    - `PubSub("topic").publish({"key": "value"})` -> Publish `{"key": "value"}` to "topic"
    """

    def __init__(self, path: str = "", topic: str = None, project=None):
        """
        Args:
        - `path` (str): Path to the topic or project. Default is "". Supported formats:
            - "projects/my-project/topics/my-topic"
            - "projects/my-project"
            - "my-project/my-topic"
            - "my-project"
        - `topic` (str): Name of the topic
        - `project` (str): Project ID
        """
        self.topic_id = None
        self.project = None
        self.path = path
        if path.startswith("projects/"):
            path = path.split("/")[1::2]
            path = "/".join(path)
            self.path = path
        try:
            self.project, self.topic_id = path.split("/")
        except ValueError:
            self.project = path if path != "" else None
        self.topic_id = self.topic_id or topic
        self.level = "topic" if self.topic_id else "project"
        self.project = self.project or project or get_default_arg("project")
        self.parent = f"projects/{self.project}"
        if self.level == "topic":
            self.parent = f"{self.parent}/topics/{self.topic_id}"

        self.pubsub = ModuleHandler("google.cloud").please_import(
            "pubsub_v1", who_is_calling="PubSub"
        )
        self.publisher = ClientHandler(self.pubsub.PublisherClient).get()
        self.types = self.pubsub.types
        self.exceptions = ModuleHandler("google.api_core").please_import(
            "exceptions", who_is_calling="PubSub"
        )
        self.topic_path = self.publisher.topic_path(self.project, self.topic_id)

    def __repr__(self):
        return f"PubSub({self.topic_id})"

    def ls_topics(self, full_name=False):
        """
        Lists all topics in a project.

        Args:
        - `full_name` (bool): If False, returns names in format "{project}/{topic}".
                              Otherwise, returns full names "projects/{project}/topics/{topic}".
                              Default is False.
        """
        topics = self.publisher.list_topics(request={"project": self.parent})
        output = [topic.name for topic in topics]
        if not full_name:
            output = ["/".join(topic.split("/")[1::2]) for topic in output]
        return output

    def ls_subscriptions(self, full_name=False):
        """
        Lists all subscriptions for a topic.

        Args:
        - `full_name` (bool): If False, returns names in format "{project}/{topic/{subscription}".
                              Otherwise, returns full names "projects/{project}/topics/{topic}/subscriptions/{subscription}".
                              Default is False.
        """
        subscriptions = self.publisher.list_topic_subscriptions(
            request={"topic": self.parent}
        )
        output = [subscription for subscription in subscriptions]
        if not full_name:
            output = [
                "/".join(subscription.split("/")[1::2]) for subscription in output
            ]
        return output

    def ls(self, full_name=False):
        if self.level == "topic":
            return self.ls_subscriptions(full_name=full_name)
        elif self.level == "project":
            return self.ls_topics(full_name=full_name)
        else:
            raise ValueError(f"Invalid level: {self.level}")

    def publish(self, data) -> None:
        if isinstance(data, dict):
            data = json.dumps(data)
        try:
            publish_future = self.publisher.publish(self.parent, data.encode("utf-8"))
            result = publish_future.result()
        except Exception as e:
            log(f"PubSub - An error occurred: {e}")

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
        schema_settings = (
            self.types.SchemaSettings(schema=schema, encoding=encoding)
            if schema
            else None
        )
        if message_retention_duration:
            duration_pb2 = ModuleHandler("google.protobuf").please_import(
                "duration_pb2", who_is_calling="PubSub"
            )
            message_retention_duration = duration_pb2.Duration(
                seconds=message_retention_duration
            )
        log(f"PubSub - Creating topic: {self.topic_id} in {self.project}...")
        topic_resource = self.types.Topic(
            name=self.parent,
            schema_settings=schema_settings,
            labels=labels,
        )
        try:
            topic = self.publisher.create_topic(request=topic_resource)
        except self.exceptions.AlreadyExists as e:
            if if_exists != "ignore":
                raise e
            log(f"PubSub - Topic already exists: {self.topic_id} in {self.project}")
            topic = self.publisher.get_topic(request={"topic": self.parent})
            return topic
        log(f"PubSub - Topic created: {topic.name} in {self.project}")
        return topic

    def delete_topic(self, errors="ignore"):
        """
        Delete a topic.
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

    def delete(self, errors="ignore"):
        if self.level == "topic":
            return self.delete_topic(errors=errors)
        elif self.level == "project":
            return self.delete_topic(errors=errors)
        else:
            raise ValueError(f"Invalid level: {self.level}")

    def create(self, **kwargs):
        if self.level == "topic":
            return self.create_topic(**kwargs)
        else:
            raise ValueError(f"Invalid level: {self.level}")


if __name__ == "__main__":
    PubSub("vitaminb16/test_topic").create_topic()
    breakpoint()
    # PubSub("vitaminb16/test_topic").delete_topic()
    # PubSub("test_topic").publish("data")
    # PubSub("test_topic").publish({"key": "value"})
