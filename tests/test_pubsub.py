import pytest
from uuid import uuid4

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

    success[19] = PubSub(topic="my-topic").level == "topic"
    success[20] = PubSub(topic="my-topic").topic_id == "my-topic"
    success[21] = PubSub(topic="my-topic").path.endswith("my-topic")

    success[22] = PubSub(subscription="my-subscription").level == "subscription"
    success[23] = (
        PubSub(subscription="my-subscription").subscription == "my-subscription"
    )
    success[24] = PubSub(subscription="my-subscription").path.endswith(
        "my-subscription"
    )

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_pubsub_publish(mocker):
    success = {}

    publisher = mocker
    p = PubSub(topic="test_topic")
    p.publish("data")  # <-- Testing this line
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


def test_pubsub_create_topic():
    success = {}

    topic_name = f"test_topic_{uuid4().hex}"

    # Doesn't exist
    success[0] = not PubSub(topic=topic_name).exists()
    success[1] = topic_name not in PubSub().ls()

    # Created
    PubSub(topic=topic_name).create()  # <-- Testing this line
    success[2] = PubSub(topic=topic_name).exists()
    success[3] = topic_name in PubSub().ls()

    # Doesn't exist
    PubSub(topic=topic_name).delete()
    success[4] = not PubSub(topic=topic_name).exists()
    success[5] = topic_name not in PubSub().ls()

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_pubsub_create_subscription():
    success = {}

    topic_name = f"test_topic_{uuid4().hex}"
    subscription_name = f"test_subscription_{uuid4().hex}"

    # Doesn't exist
    success[0] = not PubSub(subscription=subscription_name).exists()
    success[1] = not PubSub(subscription=subscription_name, topic=topic_name).exists()
    success[2] = topic_name not in PubSub().ls()

    # Created
    PubSub(topic=topic_name).create()  # <-- Testing this line
    success[3] = PubSub(topic=topic_name).exists()
    PubSub(subscription=subscription_name, topic=topic_name).create()
    success[4] = PubSub(subscription=subscription_name).exists()
    success[5] = PubSub(subscription=subscription_name, topic=topic_name).exists()
    success[6] = subscription_name in PubSub().ls_subscriptions()
    success[7] = topic_name in PubSub().ls()
    success[8] = subscription_name in PubSub(topic=topic_name).ls()

    # Doesn't exist
    PubSub(subscription=subscription_name, topic=topic_name).delete()
    success[9] = not PubSub(subscription=subscription_name).exists()
    success[10] = not PubSub(subscription=subscription_name, topic=topic_name).exists()
    success[11] = PubSub(topic=topic_name).exists()
    success[12] = topic_name in PubSub().ls()
    success[13] = subscription_name not in PubSub().ls_subscriptions()
    success[14] = subscription_name not in PubSub(topic=topic_name).ls()

    PubSub(topic=topic_name).delete()
    success[15] = not PubSub(topic=topic_name).exists()
    success[16] = topic_name not in PubSub().ls()

    failed = [k for k, v in success.items() if not v]

    assert not failed
