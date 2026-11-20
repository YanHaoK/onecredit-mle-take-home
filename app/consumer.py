"""Consume registry deliveries with manual acknowledgment and bounded retries."""

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
import logging
import random
import sys
from typing import TypeVar

import aio_pika
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage
from pamqp.commands import Basic
from psycopg import AsyncConnection, Error as DatabaseError

from app.config import MODEL_NAME, Settings
from app.embeddings import Embedder
from app.events import InvalidEvent, parse_event
from app.repository import connect, existing_outcome, store_event

SOURCE_QUEUE = "company-events"
QUARANTINE_QUEUE = "company-events.dlq"
log = logging.getLogger(__name__)
T = TypeVar("T")


async def retry(
    action: Callable[[], Awaitable[T]],
    errors: tuple[type[Exception], ...],
    operation: str,
    message_id: str | None,
) -> T:
    for attempt in range(1, 4):
        try:
            return await action()
        except errors as exc:
            log.warning(
                "delivery_error operation=%s message_id=%s attempt=%s error=%s",
                operation, message_id, attempt, type(exc).__name__,
            )
            if attempt == 3:
                raise
            await asyncio.sleep(2 ** (attempt - 1) + random.uniform(0, 0.25))
    raise AssertionError("retry loop exhausted")


async def quarantine(
    exchange: AbstractExchange, message: AbstractIncomingMessage, reason: str,
    *, model_name: str | None = None,
) -> None:
    headers = {"reason": reason}
    if model_name:
        headers["model_name"] = model_name
    confirmation = await exchange.publish(
        aio_pika.Message(
            message.body, content_type=message.content_type,
            message_id=message.message_id, headers=headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=QUARANTINE_QUEUE,
        mandatory=True,
        timeout=5,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise ConnectionError("quarantine publication was not confirmed")
    await message.ack()
    log.warning("quarantined message_id=%s reason=%s", message.message_id, reason)


async def process_delivery(
    message: AbstractIncomingMessage,
    exchange: AbstractExchange,
    database: AsyncConnection,
    embedder: Embedder,
) -> None:
    message_id = message.message_id
    try:
        event = parse_event(message.body)
    except InvalidEvent as exc:
        await quarantine(exchange, message, str(exc))
        return

    outcome = await retry(
        partial(existing_outcome, database, event, message_id),
        (DatabaseError,), "read_version", message_id,
    )
    if outcome is None:
        vector = None
        if (description := event.embedding_text) is not None:
            try:
                vector = await retry(
                    partial(asyncio.to_thread, embedder.encode, description),
                    (Exception,), "embed", message_id,
                )
            except Exception as exc:
                await quarantine(
                    exchange, message, f"embedding_failed:{type(exc).__name__}",
                    model_name=MODEL_NAME,
                )
                return

        outcome = await retry(
            partial(store_event, database, event, message_id, vector),
            (DatabaseError,), "write_version", message_id,
        )

    # store_event returns after its transaction has committed. A failed ACK
    # leaves the broker free to redeliver, where the version check is idempotent.
    await message.ack()
    log.info(
        "processed outcome=%s company=%s updated_at=%s message_id=%s",
        outcome, event.registration_number, event.updated_at, message_id,
    )


async def consume(settings: Settings, embedder: Embedder) -> None:
    delay = 1
    while True:
        try:
            async with await connect(settings.database_url) as database:
                async with await aio_pika.connect(settings.rabbitmq_url, timeout=5) as broker:
                    channel = await broker.channel(publisher_confirms=True, on_return_raises=True)
                    await channel.set_qos(prefetch_count=1)
                    source = await channel.declare_queue(SOURCE_QUEUE, durable=True, timeout=5)
                    await channel.declare_queue(QUARANTINE_QUEUE, durable=True, timeout=5)
                    log.info("consuming queue=%s", SOURCE_QUEUE)
                    async with source.iterator(no_ack=False) as deliveries:
                        async for message in deliveries:
                            await process_delivery(message, channel.default_exchange, database, embedder)
                            delay = 1
                    raise ConnectionError("source delivery stream closed")
        except (
            DatabaseError, aio_pika.exceptions.AMQPException,
            aio_pika.exceptions.ChannelInvalidStateError, OSError,
        ) as exc:
            # Closing the channel releases any unacknowledged delivery before
            # the next session. Back off before reopening broker consumption.
            log.warning("reconnecting error=%s delay=%s", type(exc).__name__, delay)
            await asyncio.sleep(delay + random.uniform(0, 0.25))
            delay = min(delay * 2, 30)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("loading model=%s", MODEL_NAME)
    embedder = Embedder()
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    try:
        asyncio.run(consume(Settings.from_env(), embedder), loop_factory=loop_factory)
    except KeyboardInterrupt:
        log.info("consumer stopped")


if __name__ == "__main__":
    main()
