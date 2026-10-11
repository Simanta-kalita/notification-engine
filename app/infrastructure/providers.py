import asyncio
import random
from app.domain.interfaces import INotificationProvider
from app.core.logging import logger

class FirebasePushProvider(INotificationProvider):
    async def send(self, user_id: str, message: str) -> bool:
        # Simulate I/O Bound Network Call to FCM
        await asyncio.sleep(random.uniform(0.05, 0.2)) 
        if random.random() < 0.02: # Simulate 2% failure rate
            raise Exception("FCM Network Timeout")
        logger.info(f"[PUSH] User {user_id}: {message}")
        return True

class TwilioSMSProvider(INotificationProvider):
    async def send(self, user_id: str, message: str) -> bool:
        # Simulate I/O Bound Network Call to Twilio
        await asyncio.sleep(random.uniform(0.1, 0.3))
        if random.random() < 0.05: # Simulate 5% failure rate
            raise Exception("Twilio API Rate Limit")
        logger.info(f"[SMS] User {user_id}: {message}")
        return True