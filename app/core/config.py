from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "notification-engine"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    REDIS_URL: str = "redis://localhost:6379/0"
    DATABASE_URL: str = "postgresql+asyncpg://notification_engine:notification_engine_pwd@localhost:5432/notification_engine_db"
    
    # Decider Rules
    INACTIVE_USER_THRESHOLD_DAYS: int = 30
    QUIET_HOURS_START: int = 22  # 10 PM
    QUIET_HOURS_END: int = 7     # 7 AM
    MAX_NOTIFICATIONS_PER_HOUR: int = 10
    # Pre-Queue Rate Limiting
    MAX_EVENTS_PER_MINUTE: int = 100 
    # Retry & DLQ Config
    MAX_RETRIES: int = 3
    RETRY_TOPIC_PREFIX: str = "notification-engine.retry"
    DLQ_TOPIC: str = "notification-engine.dlq"

    class Config:
        env_file = ".env"

settings = Settings()