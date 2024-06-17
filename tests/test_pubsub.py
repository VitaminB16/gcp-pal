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

    success[14] = PubSub("my-project/my-topic/my-subscription").level == "subscription"
    success[15] = (
        PubSub("my-project/my-topic/my-subscription").path
        == "my-project/my-topic/my-subscription"
    )
    success[16] = PubSub("my-project/my-topic/my-subscription").project == "my-project"
    success[17] = PubSub("my-project/my-topic/my-subscription").topic_id == "my-topic"
    success[18] = (
        PubSub("my-project/my-topic/my-subscription").subscription == "my-subscription"
    )

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_pubsub_publish(mocker):
    success = {}

    publisher = mocker
    p = PubSub(topic="test_topic")
    p.publish("data")
    try:
        publisher.assert_called_once()
        success[0] = True
    except AssertionError:
        success[0] = False
    try:
        publisher.return_value.publish.assert_called_once()
        success[1] = True
    except AssertionError:
        success[1] = False
    try:
        publisher.return_value.publish.assert_called_with(
            p.parent, "data".encode("utf-8")
        )
        success[2] = True
    except AssertionError:
        success[2] = False

    failed = [k for k, v in success.items() if not v]

    assert not failed
