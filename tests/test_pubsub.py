import pytest

from gcp_pal.pubsub import PubSub


@pytest.fixture
def mocker():
    from unittest.mock import patch

    with patch("google.cloud.pubsub_v1.PublisherClient") as publisher:
        yield publisher


def test_pubsub_publish(mocker):
    publisher = mocker
    p = PubSub("test_topic")
    p.publish("data")
    publisher.assert_called_once()
    publisher.return_value.publish.assert_called_once()
    publisher.return_value.publish.assert_called_with(
        p.topic_path, "data".encode("utf-8")
    )
