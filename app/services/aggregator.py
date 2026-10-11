from app.domain.interfaces import IAggregator
from app.domain.models import NotificationEvent, AggregatedNotification
from app.infrastructure.redis_cache import RedisAggregator

class NotificationAggregatorService(IAggregator):
    def __init__(self, redis_aggregator: RedisAggregator):
        self.redis_aggregator = redis_aggregator

    async def aggregate(self, event: NotificationEvent) -> AggregatedNotification:
        count, message = await self.redis_aggregator.aggregate(event)
        return AggregatedNotification(
            target_user_id=event.target_user_id,
            message=message,
            count=count,
            event_type=event.event_type,
            priority=event.priority
        )