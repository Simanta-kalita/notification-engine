from app.domain.models import NotificationEvent, UserPreference

class INotificationFilter(ABC):
    @abstractmethod
    async def apply(self, event: NotificationEvent, user: UserPreference) -> bool:
        """Returns True if the event passes this filter, False to drop it."""
        pass