from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox
from app.topology import EXCHANGE, ROUTING_KEY


async def publish_pending(session: AsyncSession, broker) -> int:
    published = 0
    while True:
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return published
        try:
            await broker.publish(event.payload, exchange=EXCHANGE, routing_key=ROUTING_KEY)
        except Exception:
            await session.rollback()
            raise
        event.published_at = datetime.now(timezone.utc)
        await session.commit()
        published += 1
