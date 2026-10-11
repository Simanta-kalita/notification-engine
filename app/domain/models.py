from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
import uuid

class PriorityLevel(str, Enum):
    P0 = "P0"  # Critical (Password reset, OTP)
    P1 = "P1"  # Standard (Likes, Comments)

class EventType(str, Enum):
    LIKE = "like"
    COMMENT = "comment"
    MENTION = "mention"

class NotificationEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    actor_id: str
    target_user_id: str
    post_id: Optional[str] = None
    content: Optional[str] = None
    priority: PriorityLevel = PriorityLevel.P1

class UserPreference(BaseModel):
    user_id: str
    notify_push: bool = True
    notify_email: bool = False
    notify_sms: bool = False
    last_active_days: int = 0
    timezone: str = "UTC"  # <-- Added for QuietHoursFilter

class AggregatedNotification(BaseModel):
    target_user_id: str
    message: str
    count: int
    event_type: EventType
    priority: PriorityLevel