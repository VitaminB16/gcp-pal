import os
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

        self.topic_path = self.publisher.topic_path(self.project, topic)

    def __repr__(self):
        return f"PubSub({self.topic_id})"

    def ls_topics(self):
        topics = self.publisher.list_topics(request={"project": self.parent})
        return [topic.name for topic in topics]

    def ls_subscriptions(self):
        subscriptions = self.publisher.list_topic_subscriptions(
            request={"topic": self.parent}
        )
        return [subscription for subscription in subscriptions]

    def ls(self):
        if self.level == "topic":
            return self.ls_subscriptions()
        elif self.level == "project":
            return self.ls_topics()
        else:
            raise ValueError(f"Invalid level: {self.level}")

    def publish(self, data) -> None:
        if isinstance(data, dict):
            data = json.dumps(data)
        try:
            publish_future = self.publisher.publish(
                self.topic_path, data.encode("utf-8")
            )
            result = publish_future.result()
        except Exception as e:
            log(f"PubSub - An error occurred: {e}")


if __name__ == "__main__":
    PubSub("projects/my-project/topics/my-topic")
    # PubSub("test_topic").publish("data")
    # PubSub("test_topic").publish({"key": "value"})
