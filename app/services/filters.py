from datetime import datetime
from zoneinfo import ZoneInfo
from app.domain.interfaces import INotificationFilter
from app.domain.models import NotificationEvent, UserPreference
from app.core.config import settings
from app.core.logging import logger

class UserPreferenceFilter(INotificationFilter):
    async def apply(self, event: NotificationEvent, user: UserPreference) -> bool:
        if not (user.notify_push or user.notify_sms or user.notify_email):
            logger.info(f"Dropped: User {user.user_id} opted out of all channels.")
            return False
        return True

class ActivityFilter(INotificationFilter):
    async def apply(self, event: NotificationEvent, user: UserPreference) -> bool:
        if user.last_active_days > settings.INACTIVE_USER_THRESHOLD_DAYS:
            logger.info(f"Dropped: User {user.user_id} inactive for {user.last_active_days} days.")
            return False
        return True

class QuietHoursFilter(INotificationFilter):
    """Prevents notifications during the user's local sleep hours."""
    async def apply(self, event: NotificationEvent, user: UserPreference) -> bool:
        # 1. Convert current UTC time to the user's local timezone
        user_tz = ZoneInfo(user.timezone) # e.g., "America/New_York" or "Asia/Kolkata"
        local_hour = datetime.now(user_tz).hour
        
        # 2. Check if the local hour falls within quiet hours
        if settings.QUIET_HOURS_START <= local_hour or local_hour <= settings.QUIET_HOURS_END:
            # Exception: P0 events (e.g., Password Reset) bypass quiet hours
            if event.priority.value == "P0":
                return True
            logger.info(f"Dropped: User {event.target_user_id} is in quiet hours ({local_hour}:00 local).")
            return False
        return True

class NotificationFatigueFilter(INotificationFilter):
    """Rate limits users to prevent app uninstalls (Redis-backed)."""
    def __init__(self, redis_client):
        self.redis = redis_client

    async def apply(self, event: NotificationEvent, user: UserPreference) -> bool:
        key = f"rate_limit:{event.target_user_id}"
        
        # Use a Redis Pipeline to ensure INCR and EXPIRE happen atomically
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, 3600, nx=True) # nx=True means "only set TTL if it doesn't exist"
            results = await pipe.execute()
            
        count = results[0]
        if count > settings.MAX_NOTIFICATIONS_PER_HOUR:
            logger.info(f"Dropped: User {event.target_user_id} hit rate limit ({count}/hr).")
            return False
            
        return True

class BlockFilter(INotificationFilter):
    """Prevents notifications if the user has blocked the actor."""
    def __init__(self, redis_client):
        self.redis = redis_client

    async def apply(self, event: NotificationEvent, user: UserPreference) -> bool:
        # Check Redis Set for direct block relationship (O(1) lookup, no Neo4j needed)
        is_blocked = await self.redis.sismember(f"blocked_by:{event.target_user_id}", event.actor_id)
        if is_blocked:
            logger.info(f"Dropped: User {event.target_user_id} has blocked actor {event.actor_id}.")
            return False
        return True