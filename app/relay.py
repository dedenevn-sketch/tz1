import asyncio
import json
import logging
from datetime import datetime, timezone

import aio_pika

from app.config import get_settings
from app.db import get_session_factory
from app.services.outbox import publish_pending
from app.topology import declare_topology

logger = logging.getLogger(__name__)


class AioPikaOutboxBroker:
    def __init__(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel

    async def publish(self, message: dict, *, exchange: str, routing_key: str) -> None:
        target = await self.channel.get_exchange(exchange)
        await target.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                timestamp=datetime.now(timezone.utc),
            ),
            routing_key=routing_key,
        )


async def relay_loop(stop: asyncio.Event) -> None:
    settings = get_settings()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        await declare_topology(channel)
        broker = AioPikaOutboxBroker(channel)
        while not stop.is_set():
            try:
                async with get_session_factory()() as session:
                    await publish_pending(session, broker)
            except Exception:
                logger.exception("outbox relay failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval_seconds)
            except TimeoutError:
                continue
    finally:
        await connection.close()
