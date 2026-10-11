from sqlalchemy import Column, String, Boolean, Integer, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class UserPreferenceTable(Base):
    __tablename__ = "user_preferences"
    user_id = Column(String, primary_key=True, index=True)
    notify_push = Column(Boolean, default=True)
    notify_email = Column(Boolean, default=False)
    notify_sms = Column(Boolean, default=False)
    last_active_days = Column(Integer, default=0)

class NotificationHistoryTable(Base):
    __tablename__ = "notification_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    target_user_id = Column(String, index=True)
    message = Column(String)
    event_type = Column(String)
    sent_at = Column(DateTime, default=datetime.utcnow)