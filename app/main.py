from fastapi import FastAPI, Depends, HTTPException, status
from contextlib import asynccontextmanager
import redis.asyncio as redis

from app.core.config import settings
from app.core.limiter import RateLimiter
from app.infrastructure.kafka_queue import KafkaPriorityQueue
from app.infrastructure.database import init_db, async_session_maker
from app.infrastructure.repositories import PostgresUserRepository
from app.domain.models import NotificationEvent, UserPreference

# Global instances
app_queue = KafkaPriorityQueue()
redis_client = None
limiter_instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of global resources (DB, Kafka, Redis).
    """
    global redis_client, limiter_instance
    
    # 1. Initialize Database Tables
    await init_db()
    
    # 2. Initialize Redis for Rate Limiting (Pre-Queue)
    redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    limiter_instance = RateLimiter(redis_client)
    
    # 3. Start Kafka Producer
    await app_queue.start()
    
    yield
    
    # 4. Graceful Shutdown
    await app_queue.stop()
    await redis_client.aclose()


app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)


def get_limiter() -> RateLimiter:
    """Dependency injector for the RateLimiter."""
    return limiter_instance


@app.post("/publish", status_code=status.HTTP_202_ACCEPTED)
async def publish_event(
    event: NotificationEvent,
    limiter: RateLimiter = Depends(get_limiter)
):
    """
    Publisher Endpoint with Pre-Queue Rate Limiting.
    
    1. Checks the rate limit BEFORE entering the Kafka queue.
    2. Publishes to Kafka if under the limit.
    3. Returns 429 Too Many Requests if the limit is breached.
    """
    # 1. Pre-Queue Rate Limiting (Razorpay Pattern)
    await limiter.check(event)
    
    # 2. Publish to Kafka (P0/P1 topic)
    await app_queue.publish(event)
    
    return {"status": "queued", "event_id": event.event_id}


@app.post("/user/preferences")
async def update_preferences(pref: UserPreference):
    """Updates user notification preferences in Postgres."""
    async with async_session_maker() as session:
        repo = PostgresUserRepository(session)
        await repo.save_preferences(pref)
    return {"status": "success"}


@app.get("/health")
async def health():
    """Health check endpoint for Docker/Kubernetes."""
    return {
        "status": "healthy", 
        "app": settings.PROJECT_NAME,
        "kafka_connected": True,  # In prod, ping Kafka here
        "redis_connected": redis_client is not None
    }