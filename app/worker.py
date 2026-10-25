import asyncio
import json
import logging
from datetime import datetime
from uuid import UUID

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel, ValidationError
from prometheus_client import Counter, start_http_server

from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("analytics-worker")
PROCESSED_EVENTS = Counter(
    "urlshortener_analytics_events_processed_total",
    "Redirect events processed by the analytics worker (replays included)",
)
DEAD_LETTER_EVENTS = Counter(
    "urlshortener_analytics_dead_letter_total",
    "Malformed redirect events routed to the dead-letter topic",
)


class RedirectEvent(BaseModel):
    event_id: UUID
    code: str
    clicked_at: datetime


async def run() -> None:
    db = await asyncpg.create_pool(
        settings.database_url,
        min_size=settings.database_pool_min_size,
        max_size=settings.database_pool_max_size,
    )
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="analytics-writer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=500,
    )
    dlq = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="url-shortener-analytics-dlq",
        acks="all",
        enable_idempotence=True,
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(),
    )
    start_http_server(8001)
    dlq_started = False
    consumer_started = False
    try:
        await dlq.start()
        dlq_started = True
        await consumer.start()
        consumer_started = True
        logger.info("Analytics worker consuming topic=%s", settings.kafka_topic)
        while True:
            records = await consumer.getmany(timeout_ms=1000, max_records=500)
            if not records:
                continue
            writes = []
            for partition_records in records.values():
                for record in partition_records:
                    try:
                        parsed = RedirectEvent.model_validate_json(record.value)
                        writes.append((str(parsed.event_id), parsed.code, parsed.clicked_at))
                    except (ValidationError, ValueError, TypeError):
                        logger.exception(
                            "Invalid redirect event; routing to dead-letter topic partition=%s offset=%s",
                            record.partition,
                            record.offset,
                        )
                        await dlq.send_and_wait(
                            f"{settings.kafka_topic}.dlq",
                            {
                                "original_topic": record.topic,
                                "partition": record.partition,
                                "offset": record.offset,
                                "value": (
                                    record.value.decode("utf-8", errors="replace")
                                    if isinstance(record.value, bytes)
                                    else repr(record.value)
                                ),
                            },
                        )
                        DEAD_LETTER_EVENTS.inc()

            if writes:
                async with db.acquire() as connection:
                    await connection.executemany(
                        """INSERT INTO clicks(event_id, code, clicked_at)
                           VALUES($1, $2, $3)
                           ON CONFLICT (event_id) DO NOTHING""",
                        writes,
                    )
                PROCESSED_EVENTS.inc(len(writes))
            await consumer.commit()
    finally:
        try:
            if consumer_started:
                await consumer.stop()
        finally:
            try:
                if dlq_started:
                    await dlq.stop()
            finally:
                await db.close()


if __name__ == "__main__":
    asyncio.run(run())
