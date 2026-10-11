import asyncio
from typing import Dict, List
from app.domain.interfaces import IDispatcher, INotificationProvider
from app.domain.models import AggregatedNotification, UserPreference
from app.core.config import settings
from app.core.logging import logger
from app.core.exceptions import DispatchFailedException

class NotificationDispatcherService(IDispatcher):
    def __init__(self, providers: Dict[str, INotificationProvider]):
        # Providers are injected via Dependency Injection (Strategy Pattern)
        self.providers = providers
        
        # Map user preference flags to provider names (Open/Closed Principle)
        self.channel_map = {
            "push": "notify_push",
            "sms": "notify_sms",
            "email": "notify_email"
        }

    async def dispatch(self, notification: AggregatedNotification, user_prefs: UserPreference) -> None:
        """
        Orchestrates delivery across multiple channels concurrently.
        Raises an exception if ALL channels fail, so the worker can trigger DLQ/retry.
        """
        tasks = []
        
        # 1. Dynamically build tasks based on user preferences
        for channel, pref_attr in self.channel_map.items():
            if getattr(user_prefs, pref_attr, False) and channel in self.providers:
                provider = self.providers[channel]
                tasks.append(self._send_with_retry(provider, notification, channel))

        if not tasks:
            logger.info(f"User {notification.target_user_id} has no channels enabled. Skipping.")
            return

        # 2. Execute all channel dispatches concurrently with a strict timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=settings.DISPATCH_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            logger.error(f"Dispatch timed out for user {notification.target_user_id}")
            raise DispatchFailedException("Global dispatch timeout exceeded")

        # 3. Evaluate results (Fail if ALL channels failed)
        failures = [res for res in results if isinstance(res, Exception)]
        successes = len(results) - len(failures)

        if successes == 0 and failures:
            # If every single channel failed, raise an exception to trigger Kafka retry/DLQ
            logger.error(f"All dispatch channels failed for user {notification.target_user_id}")
            raise DispatchFailedException(f"All channels failed: {[str(f) for f in failures]}")
        elif failures:
            # Partial success (e.g., Push worked, SMS failed). Log it, but don't fail the whole event.
            logger.warning(f"Partial dispatch failure for user {notification.target_user_id}: {failures}")

    async def _send_with_retry(self, provider: INotificationProvider, notification: AggregatedNotification, channel: str) -> bool:
        """
        Wraps provider.send() with Exponential Backoff Retries for transient failures.
        """
        max_retries = settings.MAX_DISPATCH_RETRIES
        for attempt in range(max_retries):
            try:
                await provider.send(notification.target_user_id, notification.message)
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"[{channel}] Final retry failed for {notification.target_user_id}: {str(e)}")
                    raise e # Bubble up to gather()
                
                # Exponential backoff: 1s, 2s, 4s...
                backoff = 2 ** attempt
                logger.warning(f"[{channel}] Attempt {attempt + 1} failed. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)