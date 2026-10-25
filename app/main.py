import asyncio
import json
import logging
import re
import secrets
import string
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import perf_counter

import asyncpg
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import settings
from app.metrics import (
    ANALYTICS_PUBLISH_FAILURES,
    REDIRECT_LATENCY,
    REDIRECTS,
)
from app.schemas import AnalyticsResponse, CreateLinkRequest, DailyClicks, LinkResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
CODE_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,32}$")
ALPHABET = string.ascii_letters + string.digits


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(
        settings.database_url,
        min_size=settings.database_pool_min_size,
        max_size=settings.database_pool_max_size,
    )
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.kafka_client_id,
        acks="all",
        enable_idempotence=True,
        request_timeout_ms=max(1000, int(settings.event_publish_timeout_seconds * 1000)),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(),
    )
    producer_started = False
    try:
        await app.state.producer.start()
        producer_started = True
        yield
    finally:
        try:
            if producer_started:
                await app.state.producer.stop()
        finally:
            try:
                await app.state.redis.aclose()
            finally:
                await app.state.db.close()


app = FastAPI(
    title="URL Shortener API",
    version="1.0.0",
    description="URL shortening with durable redirect analytics.",
    lifespan=lifespan,
)


def _new_code() -> str:
    return "".join(secrets.choice(ALPHABET) for _ in range(settings.code_length))


async def _invalidate_cache(request: Request, code: str) -> None:
    try:
        await request.app.state.redis.delete(f"link:{code}")
    except RedisError:
        logger.warning("Redis cache invalidation failed for code=%s", code, exc_info=True)


@app.post("/api/v1/links", response_model=LinkResponse, status_code=201)
async def create_link(payload: CreateLinkRequest, request: Request):
    if payload.expires_at and payload.expires_at <= datetime.now(UTC):
        raise HTTPException(status_code=422, detail="expires_at must be in the future")

    custom = payload.custom_code
    if custom and not CODE_PATTERN.fullmatch(custom):
        raise HTTPException(status_code=422, detail="Invalid custom code")

    db = request.app.state.db
    for attempt in range(5):
        code = custom or _new_code()
        try:
            row = await db.fetchrow(
                """INSERT INTO links(code, destination, expires_at)
                   VALUES($1, $2, $3)
                   RETURNING code, destination, created_at, expires_at""",
                code,
                str(payload.url),
                payload.expires_at,
            )
            return LinkResponse(
                code=row["code"],
                short_url=f"{str(request.base_url).rstrip('/')}/{row['code']}",
                url=row["destination"],
                created_at=row["created_at"],
                expires_at=row["expires_at"],
            )
        except asyncpg.UniqueViolationError:
            if custom:
                raise HTTPException(status_code=409, detail="Short code already exists") from None
            if attempt == 4:
                logger.error("Unable to allocate a unique short code after five attempts")
                raise HTTPException(status_code=503, detail="Unable to allocate short code") from None
    raise HTTPException(status_code=503, detail="Unable to allocate short code")


async def _load_link(request: Request, code: str):
    cache_key = f"link:{code}"
    try:
        cached = await request.app.state.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            expires_at = datetime.fromisoformat(data["expires_at"]) if data["expires_at"] else None
            return data["destination"], expires_at
    except (RedisError, ValueError, KeyError, json.JSONDecodeError):
        logger.warning("Redis cache lookup failed for code=%s; falling back to PostgreSQL", code, exc_info=True)

    row = await request.app.state.db.fetchrow(
        "SELECT destination, expires_at FROM links WHERE code = $1",
        code,
    )
    if row is None:
        return None
    expires_at = row["expires_at"]
    remaining = settings.cache_ttl_seconds
    if expires_at:
        remaining = min(remaining, max(1, int((expires_at - datetime.now(UTC)).total_seconds())))
    try:
        await request.app.state.redis.set(
            cache_key,
            json.dumps(
                {
                    "destination": row["destination"],
                    "expires_at": expires_at.isoformat() if expires_at else None,
                }
            ),
            ex=remaining,
        )
    except RedisError:
        logger.warning("Redis cache write failed for code=%s", code, exc_info=True)
    return row["destination"], expires_at


@app.get("/{code}", include_in_schema=False)
async def redirect(code: str, request: Request):
    started = perf_counter()
    try:
        result = await _load_link(request, code)
        if result is None:
            REDIRECTS.labels(outcome="not_found").inc()
            raise HTTPException(status_code=404, detail="Short link not found")
        destination, expires_at = result
        if expires_at and expires_at <= datetime.now(UTC):
            REDIRECTS.labels(outcome="expired").inc()
            raise HTTPException(status_code=410, detail="Short link has expired")

        event = {
            "event_id": str(uuid.uuid4()),
            "code": code,
            "clicked_at": datetime.now(UTC).isoformat(),
        }
        try:
            await asyncio.wait_for(
                request.app.state.producer.send_and_wait(settings.kafka_topic, event),
                timeout=settings.event_publish_timeout_seconds,
            )
        except (KafkaError, asyncio.TimeoutError):
            ANALYTICS_PUBLISH_FAILURES.inc()
            REDIRECTS.labels(outcome="analytics_unavailable").inc()
            logger.exception("Durable analytics publish failed; refusing untracked redirect code=%s", code)
            raise HTTPException(status_code=503, detail="Analytics temporarily unavailable") from None

        REDIRECTS.labels(outcome="success").inc()
        return RedirectResponse(destination, status_code=302)
    finally:
        REDIRECT_LATENCY.observe(perf_counter() - started)


@app.get("/api/v1/links/{code}/analytics", response_model=AnalyticsResponse)
async def link_analytics(code: str, request: Request, days: int = 30):
    if not 1 <= days <= 90:
        raise HTTPException(status_code=422, detail="days must be between 1 and 90")
    exists = await request.app.state.db.fetchval("SELECT 1 FROM links WHERE code = $1", code)
    if not exists:
        raise HTTPException(status_code=404, detail="Short link not found")
    since = datetime.now(UTC) - timedelta(days=days)
    rows = await request.app.state.db.fetch(
        """SELECT date_trunc('day', clicked_at) AS date, count(*)::bigint AS clicks
           FROM clicks WHERE code = $1 AND clicked_at >= $2
           GROUP BY 1 ORDER BY 1""",
        code,
        since,
    )
    total = await request.app.state.db.fetchval(
        "SELECT count(*)::bigint FROM clicks WHERE code = $1 AND clicked_at >= $2",
        code,
        since,
    )
    return AnalyticsResponse(
        code=code,
        total_clicks=total,
        daily_clicks=[DailyClicks(date=row["date"], clicks=row["clicks"]) for row in rows],
    )


@app.get("/health/live", include_in_schema=False)
async def live():
    return {"status": "alive"}


@app.get("/health/ready", include_in_schema=False)
async def ready(request: Request):
    try:
        await request.app.state.db.fetchval("SELECT 1")
    except asyncpg.PostgresError:
        logger.exception("Readiness check failed: PostgreSQL unavailable")
        raise HTTPException(status_code=503, detail="PostgreSQL unavailable") from None
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
