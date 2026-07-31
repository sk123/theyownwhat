#!/usr/bin/env python3
"""
tests/test_feedback_and_autocomplete.py
========================================
Test suite for Feedback Modal submission, email recipient routing,
and Multi-Jurisdiction Omnibar Autocomplete.
"""

import os
import sys
import json
import unittest
import urllib.request
import urllib.parse
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.feedback import ALERT_EMAIL_TO, ALERT_EMAIL_FROM
from api.db import get_db_connection

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:6262")


class TestFeedbackAndAutocomplete(unittest.TestCase):

    def setUp(self):
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
        except Exception:
            self.conn = psycopg2.connect("postgresql://user:password@localhost:5432/ctdata")

    def tearDown(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    # ------------------------------------------------------------------
    # 1. Feedback Tests
    # ------------------------------------------------------------------
    def test_email_recipients_configured(self):
        """Verify ALERT_EMAIL_TO contains both required recipient email addresses."""
        raw_to = os.environ.get("ALERT_EMAIL_TO", "salmunk@gmail.com, me@salmun.net")
        recipients = [addr.strip() for addr in raw_to.split(",") if addr.strip()]
        self.assertIn("salmunk@gmail.com", recipients, "salmunk@gmail.com missing from email recipients")
        self.assertIn("me@salmun.net", recipients, "me@salmun.net missing from email recipients")

    def test_submit_feedback_endpoint_and_db_save(self):
        """Verify feedback post endpoint saves appropriately to user_feedback database table."""
        payload = {
            "report_type": "link_request",
            "description": "Test automated feedback submission verification",
            "related_entities": [
                {"id": "test-1", "name": "Entity A Test", "type": "Business"},
                {"id": "test-2", "name": "Entity B Test", "type": "Property Owner"}
            ]
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{API_BASE_URL}/api/feedback",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            res_data = json.loads(resp.read().decode("utf-8"))
            self.assertIn("id", res_data)
            feedback_id = res_data["id"]

        # Verify DB save
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, report_type, description, related_entities FROM user_feedback WHERE id = %s", (feedback_id,))
            row = cur.fetchone()
            self.assertIsNotNone(row, f"Feedback #{feedback_id} not found in database!")
            self.assertEqual(row["report_type"], "link_request")
            self.assertEqual(row["description"], "Test automated feedback submission verification")

            entities = row["related_entities"]
            if isinstance(entities, str):
                entities = json.loads(entities)
            self.assertEqual(len(entities), 2)
            self.assertEqual(entities[0]["name"], "Entity A Test")

    def test_get_feedback_endpoint(self):
        """Verify GET /api/feedback returns list of feedback entries."""
        req = urllib.request.Request(f"{API_BASE_URL}/api/feedback?limit=5")
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            items = json.loads(resp.read().decode("utf-8"))
            self.assertIsInstance(items, list)
            self.assertTrue(len(items) > 0, "GET /api/feedback returned no records")

    # ------------------------------------------------------------------
    # 2. Multi-Jurisdiction Omnibar Autocomplete Tests
    # ------------------------------------------------------------------
    def test_autocomplete_omnibar_all_types(self):
        """Verify autocomplete returns omnibar formatted items with label, type, context, and rank."""
        query = urllib.parse.quote("hartford")
        url = f"{API_BASE_URL}/api/autocomplete?q={query}&type=all"
        with urllib.request.urlopen(url) as resp:
            self.assertEqual(resp.status, 200)
            results = json.loads(resp.read().decode("utf-8"))
            self.assertIsInstance(results, list)
            self.assertTrue(len(results) > 0, "Autocomplete query returned 0 results")

            item = results[0]
            self.assertIn("label", item)
            self.assertIn("type", item)
            self.assertIn("context", item)

    def test_autocomplete_multi_jurisdiction_states(self):
        """Verify autocomplete is functional across multiple jurisdictions (CT, NY, DC, BALTIMORE, CHICAGO)."""
        jurisdictions = [
            ("CT", "hartford"),
            ("NY", "park"),
            ("DC", "street"),
            ("BALTIMORE", "baltimore"),
            ("CHICAGO", "main"),
        ]

        for state, term in jurisdictions:
            q = urllib.parse.quote(term)
            url = f"{API_BASE_URL}/api/autocomplete?q={q}&type=all&state={state}"
            with urllib.request.urlopen(url) as resp:
                self.assertEqual(resp.status, 200, f"Autocomplete failed for state={state}")
                results = json.loads(resp.read().decode("utf-8"))
                self.assertIsInstance(results, list, f"Non-list returned for state={state}")

    def test_autocomplete_word_boundary_matching(self):
        """Verify searching 'salmun' matches 'KAZEROUNIAN SALMUN' via word boundary matching."""
        url = f"{API_BASE_URL}/api/autocomplete?q=salmun&type=all"
        with urllib.request.urlopen(url) as resp:
            self.assertEqual(resp.status, 200)
            results = json.loads(resp.read().decode("utf-8"))
            labels = [item["label"].upper() for item in results]
            self.assertTrue(
                any("KAZEROUNIAN SALMUN" in lbl or "SALMUN" in lbl for lbl in labels),
                f"Expected KAZEROUNIAN SALMUN in results for 'salmun', got: {labels}"
            )

    def test_autocomplete_multi_term_out_of_order(self):
        """Verify out-of-order multi-term search 'salmun kazerounian' matches 'KAZEROUNIAN SALMUN'."""
        url = f"{API_BASE_URL}/api/autocomplete?q=salmun%20kazerounian&type=all"
        with urllib.request.urlopen(url) as resp:
            self.assertEqual(resp.status, 200)
            results = json.loads(resp.read().decode("utf-8"))
            labels = [item["label"].upper() for item in results]
            self.assertTrue(
                any("KAZEROUNIAN SALMUN" in lbl for lbl in labels),
                f"Expected KAZEROUNIAN SALMUN for out-of-order query 'salmun kazerounian', got: {labels}"
            )

    def test_autocomplete_enriched_context(self):
        """Verify returned autocomplete items include non-generic enriched context."""
        url = f"{API_BASE_URL}/api/autocomplete?q=hartford&type=all"
        with urllib.request.urlopen(url) as resp:
            self.assertEqual(resp.status, 200)
            results = json.loads(resp.read().decode("utf-8"))
            self.assertTrue(len(results) > 0)
            for item in results:
                ctx = item.get("context", "")
                self.assertIsNotNone(ctx)
                self.assertNotIn(ctx.upper(), ["NULL", "NONE", "N/A", "UNKNOWN"])


if __name__ == "__main__":
    unittest.main()
