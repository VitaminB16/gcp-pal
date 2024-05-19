import os
import json
from gcp_pal.utils import try_import

from gcp_pal.utils import get_auth_default, log, ClientHandler, ModuleHandler


class PubSub:
    """
    Class for pushing a payload to a topic

    Examples:
    - `PubSub("topic").publish("data")` -> Publish `"data"` to "topic"
    - `PubSub("topic").publish({"key": "value"})` -> Publish `{"key": "value"}` to "topic"
    """

    def __init__(self, topic: str, project=None):
        """
        Args:
        - topic (str): Name of the topic
        - project (str): Project ID
        """
        self.topic_id = topic
        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]

        self.pubsub = ModuleHandler("google.cloud").please_import(
            "pubsub_v1", who_is_calling="PubSub"
        )
        self.publisher = ClientHandler(self.pubsub.PublisherClient).get()

        self.topic_path = self.publisher.topic_path(self.project, topic)

    def __repr__(self):
        return f"PubSub({self.topic_id})"

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
    PubSub("test_topic").publish("data")
    PubSub("test_topic").publish({"key": "value"})
