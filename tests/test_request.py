import pytest

from gcp_pal.request import Request


@pytest.fixture
def mocker():
    from unittest.mock import patch

    with patch("requests.get") as get, patch("requests.post") as post, patch(
        "requests.put"
    ) as put:
        yield get, post, put


def test_request_get(mocker):
    get, post, put = mocker
    r = Request("https://example.com")
    r.get()
    get.assert_called_once_with("https://example.com", headers=r.headers)


def test_request_post(mocker):
    get, post, put = mocker
    payload = {"key": "value"}
    r = Request("https://example.com")
    r.post(payload)
    assert post.call_count == 2 or post.call_count == 1
    post.assert_any_call("https://example.com", headers=r.headers, data=payload)


def test_request_put(mocker):
    get, post, put = mocker
    payload = {"key": "value"}
    r = Request("https://example.com")
    r.put(payload)
    put.assert_called_once_with("https://example.com", headers=r.headers, data=payload)
