"""RabbitMQ: маршрут payments.new, повтор через TTL и dead-letter queue."""

import json
import os
from datetime import timedelta
from uuid import uuid4

import aio_pika
import pytest_asyncio

from app.topology import declare_topology


@pytest_asyncio.fixture(loop_scope="session")
async def topology():
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    channel = await connection.channel()
    declared = await declare_topology(channel)
    await declared.main_queue.purge()
    await declared.retry_queue.purge()
    await declared.dlq.purge()
    yield declared
    await connection.close()


async def test_exchange_routes_payment_event_to_main_queue(topology):
    marker = str(uuid4())
    await topology.exchange.publish(
        aio_pika.Message(body=json.dumps({"marker": marker}).encode()),
        routing_key="payments.new",
    )

    incoming = await topology.main_queue.get(timeout=5)
    assert json.loads(incoming.body)["marker"] == marker
    await incoming.ack()


async def test_retry_queue_returns_message_to_main_after_ttl(topology):
    marker = str(uuid4())
    await topology.channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps({"marker": marker}).encode(),
            headers={"x-attempt": 1},
            expiration=timedelta(milliseconds=400),
        ),
        routing_key=topology.retry_queue.name,
    )

    incoming = await topology.main_queue.get(timeout=5)
    assert json.loads(incoming.body)["marker"] == marker
    assert int(incoming.headers["x-attempt"]) == 1
    await incoming.ack()


async def test_rejected_main_message_lands_in_dlq(topology):
    marker = str(uuid4())
    await topology.exchange.publish(
        aio_pika.Message(body=json.dumps({"marker": marker}).encode()),
        routing_key="payments.new",
    )

    incoming = await topology.main_queue.get(timeout=5)
    await incoming.reject(requeue=False)
    dead = await topology.dlq.get(timeout=5)
    assert json.loads(dead.body)["marker"] == marker
    await dead.ack()
