import redis.asyncio as redis
from typing import Optional, Tuple
from app.core.config import settings
from app.core.logging import logger


class RedisAggregator:
    """
    Redis-backed notification aggregator.
    
    Prevents notification fatigue by batching similar events within a
    time window (e.g., "10 people liked your post" instead of 10 separate alerts).
    
    Uses atomic INCR + EXPIRE operations to guarantee correctness across
    horizontally scaled worker replicas.
    """

    # Aggregation window: 5 minutes. After this, a new aggregation cycle begins.
    WINDOW_SECONDS: int = 300

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        Args:
            redis_client: Optional shared Redis client (recommended for
                          connection pool reuse across the worker process).
                          If None, a new client is created from settings.
        """
        if redis_client is not None:
            self.redis = redis_client
            self._owns_client = False
        else:
            self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
            self._owns_client = True

    def _build_key(self, event) -> str:
        """
        Builds a deterministic aggregation key.
        Includes target_user, event_type, and post_id so likes on different
        posts don't collide.
        """
        post_id = event.post_id or "global"
        return f"agg:{event.event_type.value}:{event.target_user_id}:{post_id}"

    def _build_message(self, event, count: int) -> str:
        """Formats the user-facing aggregation message."""
        if count == 1:
            return f"Someone {event.event_type.value}d your post."
        return f"{count} people {event.event_type.value}d your post."

    async def aggregate(self, event) -> Tuple[int, str]:
        """
        Atomically increment the aggregation counter and return the current
        count + formatted message.

        Returns:
            (count, message) tuple.
        """
        key = self._build_key(event)

        # Atomic pipeline: INCR + EXPIRE (nx=True) execute as one transaction.
        # nx=True ensures the TTL is only set on the FIRST increment,
        # so the window doesn't slide on every new event.
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, self.WINDOW_SECONDS, nx=True)
            results = await pipe.execute()

        count = int(results[0])
        message = self._build_message(event, count)

        logger.debug(
            f"[Aggregator] key={key} count={count} window={self.WINDOW_SECONDS}s"
        )

        return count, message

    async def close(self) -> None:
        """
        Close the Redis connection if this instance owns it.
        If the client was injected (shared), the owner is responsible for closing it.
        """
        if self._owns_client:
            await self.redis.aclose()