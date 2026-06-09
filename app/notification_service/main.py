"""FastAPI application entrypoint.

The lifespan establishes the broker connection, declares the full topology
idempotently (so the api can be the first process up), and exposes the work
exchange on ``app.state`` for the request handlers to publish through.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from sqlalchemy import text

from notification_service.api import routes_notifications, routes_subscribers
from notification_service.api.schemas import HealthComponent, HealthResponse
from notification_service.broker import rabbit
from notification_service.config import get_settings
from notification_service.db.session import dispose_engine, get_engine
from notification_service.services import idempotency

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    connection = await rabbit.connect()
    channel = await connection.channel()
    topology = await rabbit.declare_topology(channel)

    app.state.broker_connection = connection
    app.state.broker_channel = channel
    app.state.exchange = topology.exchange
    logger.info("API startup complete; broker topology declared")

    try:
        yield
    finally:
        await connection.close()
        await idempotency.close()
        await dispose_engine()


app = FastAPI(
    title="Notification Service",
    version="0.1.0",
    description="Mass SMS/Email dispatch with priority routing, durable queues, "
    "and business-level exactly-once delivery.",
    lifespan=lifespan,
)

app.include_router(routes_notifications.router)
app.include_router(routes_subscribers.router)


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health(response: Response) -> HealthResponse:
    components: dict[str, HealthComponent] = {}

    # Database
    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        components["database"] = HealthComponent(ok=True)
    except Exception as exc:  # noqa: BLE001 - report any failure as degraded
        components["database"] = HealthComponent(ok=False, detail=str(exc))

    # Broker
    try:
        conn = app.state.broker_connection
        ok = conn is not None and not conn.is_closed
        components["broker"] = HealthComponent(ok=ok)
    except Exception as exc:  # noqa: BLE001
        components["broker"] = HealthComponent(ok=False, detail=str(exc))

    # Redis
    try:
        await idempotency.get_redis().ping()
        components["redis"] = HealthComponent(ok=True)
    except Exception as exc:  # noqa: BLE001
        components["redis"] = HealthComponent(ok=False, detail=str(exc))

    healthy = all(c.ok for c in components.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthResponse(status="ok" if healthy else "degraded", components=components)
