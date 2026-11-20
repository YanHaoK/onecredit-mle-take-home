from datetime import datetime, timezone
import json
import unittest

from app.events import InvalidEvent, parse_event


class RegistryEvents(unittest.TestCase):
    def setUp(self):
        self.event = {
            "type": "company-upserted",
            "registrationNumber": " syn-001777 ",
            "updatedAt": "2026-08-14T09:00:00+08:00",
            "data": {
                "companyName": " Nama Syarikat ",
                "natureOfBusiness": " Pengeluaran sarung tangan getah ",
                "industry": " Pembuatan ",
                "state": None,
            },
        }

    def parse(self):
        return parse_event(json.dumps(self.event, ensure_ascii=False).encode("utf-8"))

    def test_normalizes_lookup_values_and_preserves_display_fields(self):
        event = self.parse()
        self.assertEqual(event.registration_number, "SYN-001777")
        self.assertEqual(event.updated_at, datetime(2026, 8, 14, 1, tzinfo=timezone.utc))
        self.assertEqual(event.company_name, " Nama Syarikat ")
        self.assertEqual(event.nature_of_business, " Pengeluaran sarung tangan getah ")
        self.assertEqual(event.embedding_text, "Pengeluaran sarung tangan getah")
        self.assertEqual(event.industry, " Pembuatan ")
        self.assertEqual(event.industry_key, "pembuatan")

    def test_null_and_blank_fields_are_valid(self):
        for description in (None, "  \t"):
            with self.subTest(description=description):
                self.event["data"].update(natureOfBusiness=description, industry=None)
                event = self.parse()
                self.assertIsNone(event.embedding_text)
                self.assertIsNone(event.industry_key)

    def test_invalid_envelopes_and_field_types(self):
        invalid = [
            b"{",
            b"null",
            b"\xff",
            json.dumps(dict(self.event, type="unknown")).encode(),
            json.dumps(dict(self.event, updatedAt="2026-08-14T09:00:00")).encode(),
            json.dumps(dict(self.event, data={"companyName": "Name", "industry": 9})).encode(),
            json.dumps({key: value for key, value in self.event.items() if key != "updatedAt"}).encode(),
        ]
        for body in invalid:
            with self.subTest(body=body), self.assertRaises(InvalidEvent):
                parse_event(body)
