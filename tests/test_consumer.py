import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from pamqp.commands import Basic
from psycopg import errors

from app.consumer import consume, process_delivery

VALID_BODY = (Path(__file__).resolve().parents[1] / "data" / "feed.jsonl").read_bytes().splitlines()[0]


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    def setUp(self):
        self.message = Mock(body=VALID_BODY, message_id="test", ack=AsyncMock())
        self.exchange = Mock(publish=AsyncMock())
        self.database = Mock()
        self.embedder = Mock(encode=Mock(return_value=[1.0] * 384))

    async def process(self):
        await process_delivery(self.message, self.exchange, self.database, self.embedder)

    async def test_ack_waits_for_the_write(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def write(*args):
            started.set()
            await release.wait()
            return "applied"

        with (
            patch("app.consumer.existing_outcome", return_value=None),
            patch("app.consumer.store_event", side_effect=write),
        ):
            task = asyncio.create_task(self.process())
            await asyncio.wait_for(started.wait(), timeout=2)
            self.message.ack.assert_not_awaited()
            release.set()
            await task
        self.message.ack.assert_awaited_once()

    async def test_transaction_conflicts_retry_then_ack(self):
        with (
            patch("app.consumer.existing_outcome", side_effect=[errors.SerializationFailure, None]) as read,
            patch("app.consumer.store_event", side_effect=[errors.DeadlockDetected, "applied"]) as write,
            patch("app.consumer.asyncio.sleep"),
        ):
            await self.process()
        self.assertEqual(read.await_count, 2)
        self.assertEqual(write.await_count, 2)
        self.embedder.encode.assert_called_once()
        self.message.ack.assert_awaited_once()

    async def test_exhausted_lock_retries_leave_delivery_unacknowledged(self):
        with (
            patch("app.consumer.existing_outcome", return_value=None),
            patch("app.consumer.store_event", side_effect=errors.LockNotAvailable) as write,
            patch("app.consumer.asyncio.sleep"),
            self.assertRaises(errors.LockNotAvailable),
        ):
            await self.process()
        self.assertEqual(write.await_count, 3)
        self.message.ack.assert_not_awaited()
        self.exchange.publish.assert_not_awaited()

    async def run_worker(self, db_connect):
        broker = AsyncMock()
        channel = broker.channel.return_value
        channel.default_exchange = self.exchange
        source = channel.declare_queue.return_value
        source.iterator = MagicMock()
        deliveries = source.iterator.return_value.__aenter__.return_value
        deliveries.__aiter__.return_value = [self.message]
        broker.__aenter__.return_value = broker
        with (
            patch("app.consumer.connect", db_connect),
            patch("app.consumer.aio_pika.connect", return_value=broker),
        ):
            await consume(Mock(), self.embedder)

    async def test_inference_io_error_stops_worker_with_delivery_pending(self):
        failure = ConnectionError("model runtime failed")
        self.embedder.encode.side_effect = failure
        connect = AsyncMock(side_effect=[AsyncMock(), AssertionError("unexpected reconnect")])
        with (
            patch("app.consumer.existing_outcome", return_value=None),
            patch("app.consumer.store_event") as write,
            self.assertRaisesRegex(RuntimeError, "message_id=test model=") as raised,
        ):
            await self.run_worker(connect)
        self.assertIs(raised.exception.__cause__, failure)
        write.assert_not_awaited()
        self.message.ack.assert_not_awaited()
        self.exchange.publish.assert_not_awaited()

    async def test_schema_error_stops_worker_without_retries(self):
        connect = AsyncMock(side_effect=[AsyncMock(), AssertionError("unexpected reconnect")])
        with (
            patch("app.consumer.existing_outcome", return_value=None),
            patch("app.consumer.store_event", side_effect=errors.UndefinedColumn) as write,
            patch("app.consumer.asyncio.sleep") as sleep,
            self.assertRaises(errors.UndefinedColumn),
        ):
            await self.run_worker(connect)
        connect.assert_awaited_once()
        write.assert_awaited_once()
        sleep.assert_not_awaited()
        self.message.ack.assert_not_awaited()
        self.exchange.publish.assert_not_awaited()

    async def test_lost_connection_reopens_session_after_backoff(self):
        connect = AsyncMock(side_effect=[AsyncMock(), asyncio.CancelledError])
        with (
            patch("app.consumer.existing_outcome", side_effect=errors.ConnectionFailure) as read,
            patch("app.consumer.asyncio.sleep") as sleep,
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.run_worker(connect)
        read.assert_awaited_once()
        sleep.assert_awaited_once()
        self.assertEqual(connect.await_count, 2)
        self.message.ack.assert_not_awaited()
        self.exchange.publish.assert_not_awaited()

    async def test_redelivery_after_ack_failure_skips_inference_and_write(self):
        with (
            patch("app.consumer.existing_outcome", side_effect=[None, "duplicate"]),
            patch("app.consumer.store_event", return_value="applied") as write,
        ):
            self.message.ack.side_effect = [ConnectionError, None]
            with self.assertRaises(ConnectionError):
                await self.process()
            await self.process()
        write.assert_awaited_once()
        self.embedder.encode.assert_called_once()
        self.assertEqual(self.message.ack.await_count, 2)

    async def test_quarantine_requires_publisher_confirmation(self):
        self.message.body = b"{"
        self.message.content_type = "application/json"
        self.exchange.publish.return_value = Basic.Nack()
        with self.assertRaises(ConnectionError):
            await self.process()
        self.message.ack.assert_not_awaited()

        self.exchange.publish.return_value = Basic.Ack()
        await self.process()
        published = self.exchange.publish.await_args.args[0]
        self.assertEqual(published.body, self.message.body)
        self.assertEqual(published.message_id, self.message.message_id)
        self.message.ack.assert_awaited_once()
