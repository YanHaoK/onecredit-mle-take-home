import math
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
import psycopg
from psycopg_pool import PoolTimeout

from app.api import app
from app.config import MODEL_NAME


class ApiErrors(unittest.TestCase):
    def test_liveness_and_validation(self):
        with TestClient(app) as client:
            self.assertEqual(client.get("/healthz").json(), {"status": "ok"})
            for params in ("limit=0", "limit=21", "ranking=invalid"):
                with self.subTest(params=params):
                    response = client.get(f"/companies/SYN-001777/similar?{params}")
                    self.assertEqual(response.status_code, 422)

    def test_database_unavailable(self):
        with TestClient(app) as client:
            with patch.object(app.state.pool, "connection", side_effect=PoolTimeout("private")):
                response = client.get("/companies/SYN-001777/similar")
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["detail"]["code"], "SERVICE_UNAVAILABLE")
            self.assertNotIn("private", response.text)


TEST_URL = os.getenv("ONECREDIT_TEST_DATABASE_URL")


@unittest.skipUnless(TEST_URL, "requires an isolated PostgreSQL with the pgvector schema")
class PostgresRanking(unittest.TestCase):
    def test_ranking_and_fallback_in_postgres(self):
        fixtures = [
            ("TEST-Q", "food", 1.0, MODEL_NAME),
            ("TEST-A", "logistics", 0.91, MODEL_NAME),
            ("TEST-B", "food", 0.88, MODEL_NAME),
            ("TEST-C", "food", None, None),
            ("TEST-D", "food", 1.0, "different-model"),
            ("TEST-E", None, None, None),
            ("TEST-F", "food", 0.88, MODEL_NAME),
            ("TEST-INACTIVE", "food", 1.0, MODEL_NAME),
        ]
        ids = [fixture[0] for fixture in fixtures]
        with psycopg.connect(TEST_URL, autocommit=True) as db:
            with db.transaction():
                for registration, industry, cosine, model in fixtures:
                    db.execute(
                        """
                        INSERT INTO companies
                            (registration_number, company_name, industry, industry_key,
                             nature_of_business, updated_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        """,
                        (registration, "Example", industry, industry,
                         "Description" if cosine is not None else None),
                    )
                    if cosine is not None:
                        vector = [cosine, math.sqrt(1 - cosine**2)] + [0.0] * 382
                        db.execute(
                            """
                            INSERT INTO company_embeddings
                            SELECT registration_number, %s::real[]::vector, %s, updated_at
                            FROM companies WHERE registration_number = %s
                            """,
                            (vector, model, registration),
                        )
                # Even an inactive row with an otherwise eligible vector must stay hidden.
                db.execute("UPDATE companies SET is_active = FALSE WHERE registration_number = 'TEST-INACTIVE'")
            try:
                with patch.dict(os.environ, {"DATABASE_URL": TEST_URL, "INDUSTRY_BONUS": "0.05"}):
                    with TestClient(app) as client:
                        self.check_rankings(client)
            finally:
                db.execute("DELETE FROM companies WHERE registration_number = ANY(%s)", (ids,))

    def check_rankings(self, client):
        route = "/companies/%20test-q%20/similar"
        weighted = client.get(f"{route}?limit=1")
        self.assertEqual(weighted.status_code, 200, weighted.text)
        payload = weighted.json()
        self.assertEqual(payload["query_company"], "TEST-Q")
        first = payload["recommendations"][0]
        self.assertEqual(first["registration_number"], "TEST-B")
        self.assertAlmostEqual(first["cosine_similarity"], 0.88, places=5)
        self.assertAlmostEqual(first["ranking_score"], 0.93, places=5)

        cosine = client.get(f"{route}?limit=1&ranking=cosine").json()
        self.assertEqual(cosine["recommendations"][0]["registration_number"], "TEST-A")
        self.assertEqual(cosine["industry_bonus"], 0)

        cards = client.get(f"{route}?limit=5").json()["recommendations"]
        self.assertEqual([card["registration_number"] for card in cards],
                         ["TEST-B", "TEST-F", "TEST-A", "TEST-C", "TEST-D"])
        for card in cards[3:]:
            self.assertEqual(card["match_type"], "same_industry")
            self.assertIsNone(card["cosine_similarity"])
            self.assertIsNone(card["ranking_score"])

        for registration in ("TEST-C", "TEST-D"):
            fallback = client.get(f"/companies/{registration}/similar").json()["recommendations"]
            self.assertEqual(len(fallback), 4)
            self.assertTrue(all(card["match_type"] == "same_industry" for card in fallback))
            self.assertNotIn(registration, [card["registration_number"] for card in fallback])
        self.assertEqual(client.get("/companies/TEST-E/similar").json()["recommendations"], [])
        inactive = client.get("/companies/TEST-INACTIVE/similar")
        self.assertEqual(inactive.status_code, 200)
        self.assertEqual(inactive.json()["recommendations"], [])
        missing = client.get("/companies/TEST-MISSING/similar")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["detail"]["code"], "COMPANY_NOT_FOUND")
