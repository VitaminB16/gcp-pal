import pytest

from gcp_pal.pubsub import PubSub


@pytest.fixture
def mocker():
    from unittest.mock import patch

    with patch("google.cloud.pubsub_v1.PublisherClient") as publisher:
        yield publisher


def test_pubsub_constructor():
    success = {}

    success[0] = PubSub().level == "project"
    success[1] = PubSub("projects/my-project").level == "project"
    success[2] = PubSub("projects/my-project").path == "my-project"
    success[3] = PubSub("projects/my-project").project == "my-project"
    success[4] = PubSub("projects/my-project/topics/my-topic").level == "topic"
    success[5] = (
        PubSub("projects/my-project/topics/my-topic").path == "my-project/my-topic"
    )
    success[6] = PubSub("projects/my-project/topics/my-topic").project == "my-project"
    success[7] = PubSub("projects/my-project/topics/my-topic").topic_id == "my-topic"

    success[8] = PubSub("my-project/my-topic").topic_id == "my-topic"
    success[9] = PubSub("my-project/my-topic").project == "my-project"
    success[10] = PubSub("my-project/my-topic").path == "my-project/my-topic"
    success[11] = PubSub("my-project/my-topic").level == "topic"

    success[11] = PubSub("my-project").level == "project"
    success[12] = PubSub("my-project").project == "my-project"
    success[13] = PubSub("my-project").path == "my-project"

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_pubsub_publish(mocker):
    publisher = mocker
    p = PubSub("test_topic")
    p.publish("data")
    publisher.assert_called_once()
    publisher.return_value.publish.assert_called_once()
    publisher.return_value.publish.assert_called_with(
        p.topic_path, "data".encode("utf-8")
    )
