import json
import google.auth
from google.cloud import pubsub_v1

from gcp_tools.utils import log


class PubSub:
    """
    Class for pushing a payload to a topic

    Examples:
    - PubSub("topic").publish("data") -> Publish "data" to "topic"
    - PubSub("topic").publish({"key": "value"}) -> Publish {"key": "value"} to "topic"
    """

    def __init__(self, topic: str, project=None):
        """
        Args:
        - topic (str): Name of the topic
        - project (str): Project ID
        """
        self.topic_id = topic
        self.publisher = pubsub_v1.PublisherClient()
        self.project = project or google.auth.default()[1]
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
