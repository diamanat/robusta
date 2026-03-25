import json
import logging
import threading
from typing import Dict, Optional, Protocol

from cachetools import TTLCache

from robusta.core.model.env_vars import (
    SLACK_RESOLVED_MESSAGE_STATE_BACKEND,
    SLACK_RESOLVED_MESSAGE_STATE_REDIS_PREFIX,
    SLACK_RESOLVED_MESSAGE_STATE_REDIS_URL,
    SLACK_RESOLVED_MESSAGE_STATE_TTL_SECONDS,
)

try:
    import redis
except ImportError:  # pragma: no cover - exercised only in environments without the optional dependency installed
    redis = None


RESOLVED_MESSAGE_CACHE_MAXSIZE = 10_000


class SlackMessageStateStore(Protocol):
    def set(self, channel: str, fingerprint: str, channel_id: str, ts: str) -> None:
        ...

    def get(self, channel: str, fingerprint: str) -> Optional[Dict[str, str]]:
        ...

    def delete(self, channel: str, fingerprint: str) -> None:
        ...

    def clear(self) -> None:
        ...


class InMemorySlackMessageStateStore:
    def __init__(self, ttl_seconds: int = SLACK_RESOLVED_MESSAGE_STATE_TTL_SECONDS):
        self.cache: TTLCache = TTLCache(maxsize=RESOLVED_MESSAGE_CACHE_MAXSIZE, ttl=ttl_seconds)
        self.lock = threading.RLock()

    def _key(self, channel: str, fingerprint: str) -> str:
        return f"{channel}:{fingerprint}"

    def set(self, channel: str, fingerprint: str, channel_id: str, ts: str) -> None:
        with self.lock:
            self.cache[self._key(channel, fingerprint)] = {"channel": channel_id, "ts": ts}

    def get(self, channel: str, fingerprint: str) -> Optional[Dict[str, str]]:
        with self.lock:
            return self.cache.get(self._key(channel, fingerprint))

    def delete(self, channel: str, fingerprint: str) -> None:
        with self.lock:
            self.cache.pop(self._key(channel, fingerprint), None)

    def clear(self) -> None:
        with self.lock:
            self.cache.clear()


class RedisSlackMessageStateStore:
    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int = SLACK_RESOLVED_MESSAGE_STATE_TTL_SECONDS,
        prefix: str = SLACK_RESOLVED_MESSAGE_STATE_REDIS_PREFIX,
        redis_client=None,
    ):
        if not redis_url and redis_client is None:
            raise ValueError("Redis URL must be configured when Redis-backed Slack message state is enabled")
        if redis is None and redis_client is None:
            raise ImportError("redis package is not installed")

        self.ttl_seconds = ttl_seconds
        self.prefix = prefix
        self.client = redis_client or redis.Redis.from_url(redis_url, decode_responses=True)

    def _key(self, channel: str, fingerprint: str) -> str:
        return f"{self.prefix}{channel}:{fingerprint}"

    def set(self, channel: str, fingerprint: str, channel_id: str, ts: str) -> None:
        self.client.set(
            self._key(channel, fingerprint),
            json.dumps({"channel": channel_id, "ts": ts}),
            ex=self.ttl_seconds,
        )

    def get(self, channel: str, fingerprint: str) -> Optional[Dict[str, str]]:
        payload = self.client.get(self._key(channel, fingerprint))
        if not payload:
            return None
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            logging.exception("Failed to decode cached Slack message state from Redis")
            return None

    def delete(self, channel: str, fingerprint: str) -> None:
        self.client.delete(self._key(channel, fingerprint))

    def clear(self) -> None:
        logging.warning("RedisSlackMessageStateStore.clear() is not implemented to avoid broad key deletion")


def create_slack_message_state_store() -> SlackMessageStateStore:
    backend = SLACK_RESOLVED_MESSAGE_STATE_BACKEND.lower()
    if backend == "redis":
        if not SLACK_RESOLVED_MESSAGE_STATE_REDIS_URL:
            logging.warning(
                "SLACK_RESOLVED_MESSAGE_STATE_BACKEND is 'redis' but SLACK_RESOLVED_MESSAGE_STATE_REDIS_URL is empty. "
                "Falling back to in-memory state store."
            )
            return InMemorySlackMessageStateStore()
        try:
            return RedisSlackMessageStateStore(
                redis_url=SLACK_RESOLVED_MESSAGE_STATE_REDIS_URL,
                ttl_seconds=SLACK_RESOLVED_MESSAGE_STATE_TTL_SECONDS,
                prefix=SLACK_RESOLVED_MESSAGE_STATE_REDIS_PREFIX,
            )
        except Exception:
            logging.exception("Failed to initialize Redis-backed Slack message state store. Falling back to memory.")
            return InMemorySlackMessageStateStore()

    if backend != "memory":
        logging.warning(
            "Unknown SLACK_RESOLVED_MESSAGE_STATE_BACKEND='%s'. Falling back to in-memory state store.",
            backend,
        )
    return InMemorySlackMessageStateStore()
