# app/core/limiter.py
import redis.asyncio as redis
from fastapi import HTTPException, status
from app.domain.models import NotificationEvent
from app.core.config import settings
from app.core.logging import logger


class RateLimiter:
    """
    Pre-Queue Rate Limiter (Razorpay Pattern).
    Ensures a single customer/actor cannot flood Kafka and affect other users.
    """
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def check(self, event: NotificationEvent) -> None:
        """
        Checks if the event should be allowed into the queue.
        Raises HTTPException(429) if the rate limit is breached.
        """
        # Key based on actor/target and event type (e.g., one user can't spam likes)
        # You could also use event.actor_id depending on your business logic.
        key = f"pre_queue_limit:{event.actor_id}:{event.event_type.value}"
        
        # 1-minute sliding window
        window_seconds = 60 
        
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, window_seconds, nx=True)
            results = await pipe.execute()
            
        count = int(results[0])
        
        if count > settings.MAX_EVENTS_PER_MINUTE:
            logger.warning(
                f"[Rate Limiter] Breach: Actor {event.actor_id} sent {count} "
                f"{event.event_type.value} events in {window_seconds}s."
            )
            # In the Razorpay diagram, this routes to a "Rate Limited Events" queue.
            # In our API, we reject it immediately to protect Kafka.
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Max {settings.MAX_EVENTS_PER_MINUTE} events per minute."
            )