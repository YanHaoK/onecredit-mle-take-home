"""Opt-in transaction tests using the pgvector integration database."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from psycopg import DataError

from app.consumer import process_delivery
from app.events import CompanyEvent, StrikeOff
from app.repository import connect, existing_outcome, store_event

TEST_URL = os.getenv("ONECREDIT_TEST_DATABASE_URL")
FIRST_VECTOR = [1.0, 0.0] + [0.0] * 382
SECOND_VECTOR = [0.0, 1.0] + [0.0] * 382


@unittest.skipUnless(TEST_URL, "requires an isolated PostgreSQL with the pgvector schema")
class IngestionTransactions(unittest.IsolatedAsyncioTestCase):
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    async def asyncSetUp(self):
        self.db = await connect(TEST_URL)
        self.addAsyncCleanup(self.db.close)
        self.registration = f"TEST-{uuid4().hex.upper()}"
        self.addAsyncCleanup(
            self.db.execute, "DELETE FROM companies WHERE registration_number = %s",
            (self.registration,),
        )
        self.first = CompanyEvent(
            self.registration, datetime(2026, 1, 1, tzinfo=timezone.utc),
            "Company", "Old business", "Industry", None,
        )
        self.second = replace(
            self.first, updated_at=self.first.updated_at + timedelta(days=1),
            nature_of_business="New business",
        )

    async def state(self):
        cursor = await self.db.execute(
            """
            SELECT c.updated_at, c.nature_of_business, e.source_updated_at, e.embedding::real[]
            FROM companies c LEFT JOIN company_embeddings e USING (registration_number)
            WHERE c.registration_number = %s
            """,
            (self.registration,),
        )
        return await cursor.fetchone()

    def message(self, event):
        body = json.dumps({
            "type": "company-upserted",
            "registrationNumber": event.registration_number,
            "updatedAt": event.updated_at.isoformat(),
            "data": {
                "companyName": event.company_name,
                "natureOfBusiness": event.nature_of_business,
                "industry": event.industry,
            },
        }).encode()
        return Mock(body=body, message_id="test", ack=AsyncMock())

    async def test_strike_off_survives_redelivery_and_keeps_the_profile(self):
        upsert = self.message(self.first)
        strike = Mock(message_id="strike", ack=AsyncMock(), body=json.dumps({
            "type": "company-struck-off",
            "registrationNumber": self.registration.lower(),
            "updatedAt": self.second.updated_at.isoformat(),
        }).encode())
        embedder = Mock(encode=Mock(return_value=FIRST_VECTOR))
        for delivery in (upsert, strike, upsert, strike):
            await process_delivery(delivery, Mock(), self.db, embedder)
        embedder.encode.assert_called_once_with("Old business")
        self.assertEqual(upsert.ack.await_count, 2)
        self.assertEqual(strike.ack.await_count, 2)
        self.assertEqual(await self.state(), (self.second.updated_at, "Old business", None, None))
        cursor = await self.db.execute(
            "SELECT is_active, company_name, industry FROM companies WHERE registration_number = %s",
            (self.registration,),
        )
        self.assertEqual(await cursor.fetchone(), (False, "Company", "Industry"))

    async def test_strike_off_before_profile_allows_only_a_newer_reactivation(self):
        strike = StrikeOff(self.registration, self.second.updated_at)
        self.assertEqual(await store_event(self.db, strike, "strike", None), "applied")
        self.assertEqual(await store_event(self.db, self.first, "late", FIRST_VECTOR), "stale")
        self.assertEqual(await store_event(self.db, self.second, "tie", FIRST_VECTOR), "conflict")
        newer = replace(self.second, updated_at=self.second.updated_at + timedelta(days=1))
        self.assertEqual(await store_event(self.db, newer, "reopen", SECOND_VECTOR), "applied")
        self.assertEqual(await store_event(self.db, strike, "replay", None), "stale")
        self.assertEqual(await existing_outcome(self.db, newer, "replay"), "duplicate")
        self.assertEqual(await self.state(), (newer.updated_at, "New business", newer.updated_at, SECOND_VECTOR))

    async def test_overlapping_stale_write_cannot_replace_newer_vector(self):
        await store_event(self.db, self.first, "baseline", FIRST_VECTOR)
        async with await connect(TEST_URL) as writer, await connect(TEST_URL) as rival:
            async with writer.transaction():
                await store_event(writer, self.second, "new", SECOND_VECTOR)
                self.assertEqual(
                    await self.state(),
                    (self.first.updated_at, "Old business", self.first.updated_at, FIRST_VECTOR),
                )
                pending = asyncio.create_task(
                    store_event(rival, self.first, "old", FIRST_VECTOR)
                )
                try:
                    for _ in range(100):
                        cursor = await self.db.execute(
                            "SELECT pg_blocking_pids(%s)", (rival.info.backend_pid,)
                        )
                        (blockers,) = await cursor.fetchone()
                        if writer.info.backend_pid in blockers:
                            break
                        await asyncio.sleep(0.01)
                    else:
                        self.fail("older transaction did not reach the PostgreSQL row lock")
                    self.assertFalse(pending.done())
                except BaseException:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
            self.assertEqual(await pending, "stale")
        self.assertEqual(
            await self.state(),
            (self.second.updated_at, "New business", self.second.updated_at, SECOND_VECTOR),
        )

    async def test_vector_failure_rolls_back_and_delivery_can_be_retried(self):
        await store_event(self.db, self.first, "baseline", FIRST_VECTOR)
        message = self.message(self.second)
        broken = Mock(encode=Mock(return_value=[1.0]))
        with patch("app.consumer.asyncio.sleep"), self.assertRaises(DataError):
            await process_delivery(message, Mock(), self.db, broken)
        message.ack.assert_not_awaited()
        self.assertEqual(
            await self.state(),
            (self.first.updated_at, "Old business", self.first.updated_at, FIRST_VECTOR),
        )

        repaired = Mock(encode=Mock(return_value=SECOND_VECTOR))
        await process_delivery(message, Mock(), self.db, repaired)
        message.ack.assert_awaited_once()
        self.assertEqual(
            await self.state(),
            (self.second.updated_at, "New business", self.second.updated_at, SECOND_VECTOR),
        )

    async def test_commit_survives_failed_ack_and_retry_skips_inference(self):
        message = self.message(self.first)
        message.ack.side_effect = ConnectionError("simulated loss before ACK")
        embedder = Mock(encode=Mock(return_value=FIRST_VECTOR))
        with self.assertRaises(ConnectionError):
            await process_delivery(message, Mock(), self.db, embedder)
        self.assertEqual(
            await self.state(),
            (self.first.updated_at, "Old business", self.first.updated_at, FIRST_VECTOR),
        )
        redelivery = self.message(self.first)
        await process_delivery(redelivery, Mock(), self.db, embedder)
        redelivery.ack.assert_awaited_once()
        embedder.encode.assert_called_once_with("Old business")
