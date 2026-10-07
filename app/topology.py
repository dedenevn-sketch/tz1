from dataclasses import dataclass

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

EXCHANGE = "payments"
DLX = "payments.dlx"
MAIN_QUEUE = "payments.new"
RETRY_QUEUE = "payments.new.retry"
DLQ = "payments.new.dlq"
ROUTING_KEY = "payments.new"


@dataclass(frozen=True)
class DeclaredTopology:
    channel: AbstractChannel
    exchange: AbstractExchange
    dlx: AbstractExchange
    main_queue: AbstractQueue
    retry_queue: AbstractQueue
    dlq: AbstractQueue


async def declare_topology(channel: AbstractChannel) -> DeclaredTopology:
    exchange = await channel.declare_exchange(EXCHANGE, ExchangeType.DIRECT, durable=True)
    dlx = await channel.declare_exchange(DLX, ExchangeType.DIRECT, durable=True)

    dlq = await channel.declare_queue(DLQ, durable=True)
    await dlq.bind(dlx, routing_key=DLQ)

    retry_queue = await channel.declare_queue(
        RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY,
        },
    )
    main_queue = await channel.declare_queue(
        MAIN_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": DLQ,
        },
    )
    await main_queue.bind(exchange, routing_key=ROUTING_KEY)
    return DeclaredTopology(
        channel=channel,
        exchange=exchange,
        dlx=dlx,
        main_queue=main_queue,
        retry_queue=retry_queue,
        dlq=dlq,
    )
