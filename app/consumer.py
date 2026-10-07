import logging
from datetime import timedelta

import aio_pika
from faststream import FastStream
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue

from app.config import get_settings
from app.db import get_session_factory
from app.services.processing import RandomPaymentGateway, process_delivery
from app.topology import DLQ, DLX, EXCHANGE, MAIN_QUEUE, ROUTING_KEY, declare_topology

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()
broker = RabbitBroker(settings.rabbitmq_url)
payments_exchange = RabbitExchange(EXCHANGE, type=ExchangeType.DIRECT, durable=True)
payments_queue = RabbitQueue(
    MAIN_QUEUE,
    durable=True,
    routing_key=ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX,
        "x-dead-letter-routing-key": DLQ,
    },
)
app = FastStream(broker)


class HttpWebhook:
    def __init__(self, timeout: float):
        self.timeout = timeout

    async def send(self, url: str, payload: dict) -> None:
        import httpx

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()


class FastStreamDeliveryBroker:
    def __init__(self, rabbit: RabbitBroker):
        self.rabbit = rabbit

    async def publish(self, message, *, queue: str, headers: dict, expiration: int | None) -> None:
        await self.rabbit.publish(
            message,
            queue=queue,
            headers=headers,
            expiration=None if expiration is None else timedelta(milliseconds=expiration),
            persist=True,
        )


def build_gateway() -> RandomPaymentGateway:
    current = get_settings()
    return RandomPaymentGateway(
        success_rate=current.process_success_rate,
        delay_min=current.process_delay_min_seconds,
        delay_max=current.process_delay_max_seconds,
    )


@app.on_startup
async def declare_broker_topology(**_kwargs) -> None:
    connection = await aio_pika.connect_robust(get_settings().rabbitmq_url)
    try:
        channel = await connection.channel()
        await declare_topology(channel)
    finally:
        await connection.close()


@broker.subscriber(payments_queue, payments_exchange)
async def on_payment(payload: dict, message: RabbitMessage) -> None:
    current = get_settings()
    await process_delivery(
        payload,
        dict(message.headers or {}),
        session_factory=get_session_factory(),
        gateway=build_gateway(),
        webhook=HttpWebhook(current.webhook_timeout_seconds),
        broker=FastStreamDeliveryBroker(broker),
        max_attempts=current.consumer_max_attempts,
        backoff_base_seconds=current.consumer_backoff_base_seconds,
    )
