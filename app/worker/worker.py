import asyncio
import redis.asyncio as redis

from app.core.config import settings
from app.core.logging import logger
from app.core.exceptions import DispatchFailedException

# ==========================================
# Infrastructure Imports
# ==========================================
from app.infrastructure.kafka_queue import KafkaPriorityQueue
from app.infrastructure.redis_cache import RedisAggregator
from app.infrastructure.providers import FirebasePushProvider, TwilioSMSProvider
from app.infrastructure.database import async_session_maker
from app.infrastructure.repositories import (
    PostgresUserRepository,
    PostgresHistoryRepository,
)

# ==========================================
# Service Layer Imports
# ==========================================
from app.services.decider import NotificationDecider
from app.services.aggregator import NotificationAggregatorService
from app.services.dispatcher import NotificationDispatcherService

# ==========================================
# Filter Imports (Chain of Responsibility)
# ==========================================
from app.services.filters import (
    BlockFilter,
    UserPreferenceFilter,
    ActivityFilter,
    QuietHoursFilter,
    NotificationFatigueFilter,
)

# ==========================================
# 🛡️ Global Semaphore to throttle async DB writes
# Prevents spawning thousands of tasks that exhaust the Postgres pool
# ==========================================
DB_WRITE_SEMAPHORE = asyncio.Semaphore(50)  # Max 50 concurrent DB writes


async def _async_log_history(
    target_user_id: str, 
    message: str, 
    event_type: str, 
    event_id: str
) -> None:
    """
    Background task to write notification history to Postgres.
    Creates its own DB session because the parent session will close
    before this task completes.
    """
    async with DB_WRITE_SEMAPHORE:
        try:
            async with async_session_maker() as session:
                history_repo = PostgresHistoryRepository(session)
                await history_repo.log_notification(
                    target_user_id=target_user_id,
                    message=message,
                    event_type=event_type,
                )
        except Exception as e:
            # We catch this so a background DB failure doesn't crash the worker.
            # In production, you might want to push this to a separate error queue.
            logger.error(f"Background DB write failed for Event {event_id}: {e}")


async def main():
    logger.info("Starting Notification Engine Worker...")

    # ==========================================
    # 1. Initialize Global Infrastructure
    # ==========================================
    
    # Kafka Message Queue (P0/P1 priority polling + DLQ)
    queue = KafkaPriorityQueue()
    await queue.start()

    # Shared Redis Client (Used for Aggregation AND Rate Limiting AND Block checks)
    redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_aggregator = RedisAggregator(redis_client)

    # External Providers (Strategy Pattern)
    providers = {
        "push": FirebasePushProvider(),
        "sms": TwilioSMSProvider(),
    }

    # ==========================================
    # 2. Initialize Stateless Services (Reused across events)
    # ==========================================
    aggregator = NotificationAggregatorService(redis_aggregator)
    dispatcher = NotificationDispatcherService(providers=providers)

    logger.info("Worker initialized. Listening for events...")

    # ==========================================
    # 3. The Consumer Listener Loop
    # ==========================================
    while True:
        event = None  # Reset for each iteration (avoids stale state in exception block)
        try:
            # ---------------------------------------------------------
            # A. Consume from Kafka (P0 prioritized internally with timeout)
            # ---------------------------------------------------------
            event = await queue.consume()
            logger.info(
                f"Consumed Event: {event.event_id} | "
                f"Type: {event.event_type.value} | "
                f"Priority: {event.priority.value}"
            )

            # ---------------------------------------------------------
            # B. Create a fresh DB session scoped to this event
            # ---------------------------------------------------------
            async with async_session_maker() as session:
                user_repo = PostgresUserRepository(session)

                # -----------------------------------------------------
                # C. Fetch User Preferences ONCE (avoids redundant DB calls)
                # -----------------------------------------------------
                user_prefs = await user_repo.get_preferences(event.target_user_id)

                # -----------------------------------------------------
                # D. Build Filter Pipeline (Chain of Responsibility)
                # -----------------------------------------------------
                filters = [
                    BlockFilter(redis_client),
                    UserPreferenceFilter(),
                    ActivityFilter(),
                    QuietHoursFilter(),
                    NotificationFatigueFilter(redis_client),
                ]
                decider = NotificationDecider(filters=filters)

                # -----------------------------------------------------
                # 1. Run Decider Logic (uses pre-fetched user_prefs)
                # -----------------------------------------------------
                if not await decider.should_send(event, user_prefs):
                    logger.info(f"Event {event.event_id} dropped by Decider.")
                    await queue.ack(event)
                    continue

                # -----------------------------------------------------
                # 2. Aggregation Logic (Redis-backed)
                # -----------------------------------------------------
                aggregated_notification = await aggregator.aggregate(event)
                logger.info(
                    f"Aggregated Message: '{aggregated_notification.message}' "
                    f"(Count: {aggregated_notification.count})"
                )

                # -----------------------------------------------------
                # 3. Dispatch Logic (Retries + Timeout handled internally)
                # Raises DispatchFailedException if ALL channels fail
                # -----------------------------------------------------
                await dispatcher.dispatch(aggregated_notification, user_prefs)

                # -----------------------------------------------------
                # 4. 🔥 ASYNC DB WRITE: Fire-and-Forget
                # -----------------------------------------------------
                # We spawn a background task that creates its OWN session.
                # This allows the worker to ack Kafka immediately.
                asyncio.create_task(
                    _async_log_history(
                        target_user_id=event.target_user_id,
                        message=aggregated_notification.message,
                        event_type=event.event_type.value,
                        event_id=event.event_id
                    )
                )

            # ---------------------------------------------------------
            # E. Acknowledge successful processing (commit Kafka offset)
            # ---------------------------------------------------------
            await queue.ack(event)
            logger.info(f"Successfully processed Event {event.event_id}")

        # =============================================================
        # EXCEPTION HANDLING
        # =============================================================
        except asyncio.CancelledError:
            # Graceful shutdown (SIGTERM from Docker/K8s)
            logger.info("Worker shutting down gracefully...")
            await queue.stop()
            await redis_client.aclose()
            break

        except DispatchFailedException as e:
            # Permanent dispatch failure (in-memory retries exhausted)
            # Route to Retry Topic (starts at count=0) or DLQ based on nack logic
            logger.error(f"Dispatch failed permanently for Event {event.event_id}: {str(e)}")
            if event is not None:
                # Start with retry_count=0, the consumer of the retry topic will increment it
                await queue.nack(event, retry_count=0)

        except Exception as e:
            # Catch-all for unexpected errors (DB connection loss, Pydantic errors, etc.)
            event_id = event.event_id if event else "Unknown"
            logger.error(f"Unexpected Worker Error processing Event {event_id}: {str(e)}")

            if event is not None:
                await queue.nack(event)

            # Global backoff to prevent hot loop if Kafka/DB is completely down
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())