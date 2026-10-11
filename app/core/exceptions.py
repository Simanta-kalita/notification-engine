# app/core/exceptions.py
class NotificationEngineException(Exception):
    """Base exception for all notification-engine errors."""
    pass

class DispatchFailedException(NotificationEngineException):
    """Raised when all notification channels fail after retries."""
    pass

class InvalidEventException(NotificationEngineException):
    """Raised when an event fails validation."""
    pass