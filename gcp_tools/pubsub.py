import os
import json
from google.cloud import pubsub_v1

from gcp_tools.utils import log, get_default_project


class PubSub:
    """
    Class for pushing a payload to a topic

    Examples:
    - PubSub("topic").publish("data") -> Publish "data" to "topic"
    - PubSub("topic").publish({"key": "value"}) -> Publish {"key": "value"} to "topic"
    """

    _clients = {}

    def __init__(self, topic: str, project=None):
        """
        Args:
        - topic (str): Name of the topic
        - project (str): Project ID
        """
        self.topic_id = topic
        self.project = project or os.environ.get("PROJECT") or get_default_project()

        # Initialize the publisher client only once per project
        if self.project in PubSub._clients:
            self.publisher = PubSub._clients[self.project]
        else:
            self.publisher = pubsub_v1.PublisherClient()
            PubSub._clients[self.project] = self.publisher

        self.topic_path = self.publisher.topic_path(self.project, topic)

    def publish(self, data) -> None:
        if isinstance(data, dict):
            data = json.dumps(data)
        try:
            publish_future = self.publisher.publish(
                self.topic_path, data.encode("utf-8")
            )
            result = publish_future.result()
        except Exception as e:
            log(f"An error occurred: {e}")


if __name__ == "__main__":
    PubSub("test_topic").publish("data")
    PubSub("test_topic").publish({"key": "value"})
