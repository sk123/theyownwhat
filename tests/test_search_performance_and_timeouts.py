import os
import time
import unittest
from unittest.mock import patch

import psycopg2
from fastapi.testclient import TestClient

from api.main import app


class SearchPerformanceAndTimeoutsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cm = TestClient(app)
        cls.client = cls._cm.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._cm.__exit__(None, None, None)

    def test_fast_omnisearch_returns_quickly_on_large_states_and_cities(self):
        """
        Regression test for the 19-way landing omni-search DB pool exhaustion incident.
        Zero-hit and hit searches across multi-million-row states and major cities must
        stay indexed and complete well under 1.5s each.
        """
        for jur in ("ma", "ny", "fl", "nc", "il", "oh", "nyc", "chicago", "philadelphia", "miami"):
            t0 = time.time()
            res = self.client.get(f"/api/{jur}/search", params={"q": "WinnCompanies", "limit": 2, "fast": 1})
            elapsed_ms = (time.time() - t0) * 1000.0
            self.assertEqual(res.status_code, 200, f"{jur} returned {res.status_code}: {res.text[:200]}")
            self.assertIsInstance(res.json(), list)
            self.assertLess(
                elapsed_ms,
                1500.0,
                f"/api/{jur}/search?q=WinnCompanies&fast=1 took {elapsed_ms:.1f}ms (expected <1500ms)",
            )

    def test_ma_winncompanies_returns_portfolio_results(self):
        res = self.client.get("/api/ma/search", params={"q": "WinnCompanies", "limit": 5, "fast": 1})
        self.assertEqual(res.status_code, 200)
        rows = res.json()
        self.assertGreater(len(rows), 0, "Expected WinnCompanies matches in Massachusetts")

    def test_single_jurisdiction_full_search_stays_bounded(self):
        t0 = time.time()
        res = self.client.get("/api/ma/search", params={"q": "WinnCompanies", "limit": 10})
        elapsed_ms = (time.time() - t0) * 1000.0
        self.assertEqual(res.status_code, 200)
        self.assertGreater(len(res.json()), 0)
        self.assertLess(elapsed_ms, 3000.0, f"Full MA search took {elapsed_ms:.1f}ms")

    def test_search_gracefully_returns_empty_on_query_timeout_or_lock(self):
        with patch("api.city_routes._query", side_effect=psycopg2.errors.LockNotAvailable("lock timeout")):
            res = self.client.get("/api/ma/search", params={"q": "WinnCompanies", "limit": 2, "fast": 1})
            self.assertEqual(res.status_code, 200)
            self.assertEqual(res.json(), [])

    def test_pool_exhaustion_returns_503_not_500(self):
        with patch("api.db.db_pool.getconn", side_effect=psycopg2.pool.PoolError("connection pool exhausted")):
            res = self.client.get("/api/ma/stats")
            self.assertEqual(res.status_code, 503)


if __name__ == "__main__":
    unittest.main()
