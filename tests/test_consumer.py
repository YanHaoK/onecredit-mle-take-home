import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

from pamqp.commands import Basic
from psycopg import OperationalError

from app.consumer import process_delivery

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

    async def test_database_failure_retries_without_ack_or_quarantine(self):
        with (
            patch("app.consumer.existing_outcome", return_value=None),
            patch("app.consumer.store_event", side_effect=OperationalError) as write,
            patch("app.consumer.asyncio.sleep"),
            self.assertRaises(OperationalError),
        ):
            await self.process()
        self.assertEqual(write.await_count, 3)
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
