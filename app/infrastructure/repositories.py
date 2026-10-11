from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.domain.interfaces import IUserRepository, INotificationHistoryRepository
from app.domain.models import UserPreference
from app.infrastructure.models_db import UserPreferenceTable, NotificationHistoryTable

class PostgresUserRepository(IUserRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_preferences(self, user_id: str) -> UserPreference:
        result = await self.session.execute(
            select(UserPreferenceTable).where(UserPreferenceTable.user_id == user_id)
        )
        user = result.scalar_one_or_none()
        if not user:
            return UserPreference(user_id=user_id)
        return UserPreference(
            user_id=user.user_id,
            notify_push=user.notify_push,
            notify_email=user.notify_email,
            notify_sms=user.notify_sms,
            last_active_days=user.last_active_days
        )

    async def save_preferences(self, pref: UserPreference) -> None:
        result = await self.session.execute(
            select(UserPreferenceTable).where(UserPreferenceTable.user_id == pref.user_id)
        )
        user = result.scalar_one_or_none()
        if user:
            user.notify_push = pref.notify_push
            user.notify_email = pref.notify_email
            user.notify_sms = pref.notify_sms
            user.last_active_days = pref.last_active_days
        else:
            user = UserPreferenceTable(
                user_id=pref.user_id,
                notify_push=pref.notify_push,
                notify_email=pref.notify_email,
                notify_sms=pref.notify_sms,
                last_active_days=pref.last_active_days
            )
            self.session.add(user)
        await self.session.commit()

class PostgresHistoryRepository(INotificationHistoryRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def log_notification(self, target_user_id: str, message: str, event_type: str) -> None:
        history = NotificationHistoryTable(
            target_user_id=target_user_id,
            message=message,
            event_type=event_type
        )
        self.session.add(history)
        await self.session.commit()