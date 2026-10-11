from typing import List
from app.domain.interfaces import INotificationFilter
from app.domain.models import NotificationEvent
from app.core.logging import logger

class NotificationDecider:
    def __init__(self, filters: List[INotificationFilter]):
        self.filters = filters

    async def should_send(self, event: NotificationEvent, user: UserPreference) -> bool:
        """
        Runs the event through the Chain of Responsibility.
        If any filter fails, the notification is dropped.
        """
        for filter_obj in self.filters:
            try:
                # Pass both event and user (which was fetched once in worker.py)
                passes = await filter_obj.apply(event, user)
                if not passes:
                    return False
            except Exception as e:
                # Fail-safe: If a filter crashes, log it but don't crash the worker.
                # Depending on business logic, you might want to fail-open (return True) 
                # or fail-closed (return False). Here we fail-closed for safety.
                logger.error(f"Filter {filter_obj.__class__.__name__} failed: {str(e)}")
                return False # Fail-closed
        return True