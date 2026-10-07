import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from app.api.payments import router
from app.config import get_settings
from app.relay import relay_loop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    relay = None
    if get_settings().outbox_relay_enabled:
        relay = asyncio.create_task(relay_loop(stop))
        logger.info("outbox relay started")
    yield
    if relay is not None:
        stop.set()
        relay.cancel()
        with suppress(asyncio.CancelledError):
            await relay


def create_app() -> FastAPI:
    app = FastAPI(title="Payment processing", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
