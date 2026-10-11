import json
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
from app.domain.interfaces import IMessageQueue
from app.domain.models import NotificationEvent, PriorityLevel
from app.core.config import settings
from app.core.logging import logger

class KafkaPriorityQueue(IMessageQueue):
    """
    Production-ready Kafka implementation with P0/P1 Priority Polling,
    Manual Offset Commits, DLQ handling, and Partitioning Keys.
    """
    def __init__(self):
        # 1. Producer with partitioning key support
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v.model_dump()).encode('utf-8')
        )
        
        self.p0_topic = 'notification-engine.p0'
        self.p1_topic = 'notification-engine.p1'
        self.dlq_topic = 'notification-engine.dlq'

        # 2. Consumers with manual offset commits (enable_auto_commit=False)
        self.consumer_p0 = AIOKafkaConsumer(
            self.p0_topic, 
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id='notification-engine-workers-p0', # Separate group for P0
            auto_offset_reset='earliest',
            enable_auto_commit=False, # CRITICAL: We will manually commit
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.consumer_p1 = AIOKafkaConsumer(
            self.p1_topic, 
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id='notification-engine-workers-p1', # Separate group for P1
            auto_offset_reset='earliest',
            enable_auto_commit=False, # CRITICAL: We will manually commit
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v.model_dump()).encode('utf-8')
        )
        
        # Store the raw Kafka message to commit offsets later
        self._current_p0_msg = None
        self._current_p1_msg = None

    async def start(self):
        await self.producer.start()
        await self.consumer_p0.start()
        await self.consumer_p1.start()
        await self.dlq_producer.start()

    async def stop(self):
        await self.producer.stop()
        await self.consumer_p0.stop()
        await self.consumer_p1.stop()
        await self.dlq_producer.stop()

    async def publish(self, event: NotificationEvent) -> None:
        topic = self.p0_topic if event.priority == PriorityLevel.P0 else self.p1_topic
        # CRITICAL: Use target_user_id as the key to ensure ordered processing per user
        await self.producer.send_and_wait(
            topic, 
            key=event.target_user_id.encode('utf-8'), 
            value=event
        )

    async def consume(self) -> NotificationEvent:
        """
        Polls P0 first with a short timeout to prevent starving P1.
        """
        try:
            # 1. Try P0 with a 100ms timeout
            msg_p0 = await asyncio.wait_for(self.consumer_p0.getone(), timeout=0.1)
            if msg_p0:
                self._current_p0_msg = msg_p0
                return NotificationEvent(**msg_p0.value)
        except asyncio.TimeoutError:
            pass # P0 is empty, fall back to P1

        # 2. Fallback to P1 (Blocks indefinitely if both are empty)
        msg_p1 = await self.consumer_p1.getone()
        self._current_p1_msg = msg_p1
        return NotificationEvent(**msg_p1.value)

    async def ack(self, event: NotificationEvent) -> None:
        """
        Manually commit the offset to Kafka after successful processing.
        """
        if self._current_p0_msg:
            await self.consumer_p0.commit()
            self._current_p0_msg = None
        elif self._current_p1_msg:
            await self.consumer_p1.commit()
            self._current_p1_msg = None

    async def nack(self, event: NotificationEvent, retry_count: int = 0) -> None:
        """
        Handles failed events using the Retry Topic Pattern.
        - If retries < MAX_RETRIES: Routes to a retry topic (e.g., notification-engine.retry.1)
        - If retries >= MAX_RETRIES: Routes to the final Dead Letter Queue (DLQ)
        - Always commits the original Kafka offset to prevent infinite redelivery.
        """
        try:
            # 1. Determine destination based on retry count
            if retry_count >= settings.MAX_RETRIES:
                # Max retries reached -> Send to final DLQ
                target_topic = settings.DLQ_TOPIC
                log_message = f"Event {event.event_id} moved to DLQ after {retry_count} retries."
                log_level = "error"
            else:
                # Route to a dedicated retry topic (e.g., notification-engine.retry.1)
                target_topic = f"{settings.RETRY_TOPIC_PREFIX}.{retry_count}"
                log_message = f"Event {event.event_id} routed to {target_topic} (Attempt {retry_count + 1}/{settings.MAX_RETRIES})."
                log_level = "warning"

            # 2. Publish to the target topic
            # Note: We use the DLQ producer here because it shares the same serializer,
            # but in a truly separate deployment, you'd use a dedicated retry producer.
            await self.dlq_producer.send_and_wait(
                target_topic,
                key=event.target_user_id.encode('utf-8'),
                value=event
            )
            
            # 3. Log based on severity
            if log_level == "error":
                logger.error(log_message)
            else:
                logger.warning(log_message)

            # 4. CRITICAL: Commit the original offset so Kafka stops redelivering it
            # We ONLY commit after successfully publishing to the retry/DLQ topic.
            if self._current_p0_msg:
                await self.consumer_p0.commit()
                self._current_p0_msg = None
            elif self._current_p1_msg:
                await self.consumer_p1.commit()
                self._current_p1_msg = None

        except Exception as e:
            # Catch broad exceptions (KafkaError, serialization errors, etc.)
            logger.error(f"CRITICAL: Failed to nack event {event.event_id} to {target_topic}. Error: {e}")
            logger.error("Not committing offset. Kafka will redeliver this message.")
            # We do NOT commit the offset here. If publishing to the retry topic fails,
            # we want Kafka to redeliver the original message so we don't lose it.

    async def publish_rate_limited(self, event: NotificationEvent):
        # Send to a separate topic where events wait for a cooldown period
        await self.producer.send_and_wait("notification-engine.rate-limited", value=event)