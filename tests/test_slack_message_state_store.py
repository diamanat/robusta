import json
from unittest.mock import Mock, patch

from robusta.integrations.slack.message_state_store import (
    InMemorySlackMessageStateStore,
    RedisSlackMessageStateStore,
    create_slack_message_state_store,
)


def test_in_memory_message_state_store_roundtrip():
    store = InMemorySlackMessageStateStore(ttl_seconds=60)

    store.set("alerts", "fp-1", "C123", "111.222")

    assert store.get("alerts", "fp-1") == {"channel": "C123", "ts": "111.222"}

    store.delete("alerts", "fp-1")

    assert store.get("alerts", "fp-1") is None


def test_redis_message_state_store_roundtrip():
    mock_client = Mock()
    mock_client.get.return_value = json.dumps({"channel": "C123", "ts": "111.222"})
    store = RedisSlackMessageStateStore(
        redis_url="redis://example",
        ttl_seconds=60,
        prefix="test:",
        redis_client=mock_client,
    )

    store.set("alerts", "fp-1", "C123", "111.222")

    mock_client.set.assert_called_once_with(
        "test:alerts:fp-1",
        json.dumps({"channel": "C123", "ts": "111.222"}),
        ex=60,
    )
    assert store.get("alerts", "fp-1") == {"channel": "C123", "ts": "111.222"}

    store.delete("alerts", "fp-1")

    mock_client.delete.assert_called_once_with("test:alerts:fp-1")


def test_factory_falls_back_to_memory_when_redis_url_missing():
    with patch("robusta.integrations.slack.message_state_store.SLACK_RESOLVED_MESSAGE_STATE_BACKEND", "redis"):
        with patch("robusta.integrations.slack.message_state_store.SLACK_RESOLVED_MESSAGE_STATE_REDIS_URL", ""):
            store = create_slack_message_state_store()

    assert isinstance(store, InMemorySlackMessageStateStore)
