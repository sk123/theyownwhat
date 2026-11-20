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

from unittest.mock import MagicMock, patch

from api.feedback import (
    ALERT_EMAIL_TO,
    ALERT_EMAIL_FROM,
    format_general_feedback_email,
    get_feedback_recipients,
    send_feedback_notification_email,
)
from api.reviewer_feedback_routes import (
    format_revert_notification,
    format_review_report_notification,
)
from api.db import get_db_connection

def _resolve_api_base_url() -> str:
    env_url = os.environ.get("API_BASE_URL")
    if env_url:
        return env_url.rstrip("/")
    for candidate in ("http://localhost:6262", "http://ctdata_api:8000", "http://localhost:8000"):
        try:
            with urllib.request.urlopen(f"{candidate}/api/health", timeout=2) as resp:
                if resp.status == 200:
                    return candidate
        except Exception:
            continue
    return "http://localhost:6262"


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
API_BASE_URL = _resolve_api_base_url()


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
        """Verify feedback notification routing always includes salmun@storrsmagic.org plus existing alerts."""
        recipients = get_feedback_recipients()
        self.assertIn("salmun@storrsmagic.org", recipients, "salmun@storrsmagic.org missing from feedback recipients")
        self.assertIn("salmunk@gmail.com", recipients, "salmunk@gmail.com missing from email recipients")
        self.assertIn("me@salmun.net", recipients, "me@salmun.net missing from email recipients")

    def test_general_feedback_email_formatting_and_dispatch(self):
        """Verify general /api/feedback email carries full details and dispatches to salmun@storrsmagic.org."""
        subject, text_body, html_body = format_general_feedback_email(
            42,
            "mismatch",
            "Two unrelated landlords merged via shared UPS box.",
            [{"id": "net_1", "name": "ACME REALTY LLC", "type": "Ownership Network", "jurisdiction": "MA"}],
        )
        self.assertIn("#42", subject)
        self.assertIn("mismatch", subject)
        self.assertIn("ACME REALTY LLC", text_body)
        self.assertIn("jurisdiction=MA", text_body)
        self.assertIn("Two unrelated landlords merged via shared UPS box.", html_body)

        mock_smtp = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.__enter__.return_value = mock_smtp
        with patch.dict(
            os.environ,
            {
                "ALERT_EMAIL_FROM": "sender@example.com",
                "ALERT_EMAIL_PASS": "abcd efgh ijkl mnop",
                "DISABLE_FEEDBACK_EMAILS": "",
            },
        ), patch("api.feedback.smtplib.SMTP", return_value=mock_ctx):
            ok = send_feedback_notification_email(subject, text_body, html_body=html_body)

        self.assertTrue(ok)
        mock_smtp.login.assert_called_once_with("sender@example.com", "abcdefghijklmnop")
        send_args = mock_smtp.sendmail.call_args[0]
        self.assertIn("salmun@storrsmagic.org", send_args[1])
        import email as email_stdlib
        parsed = email_stdlib.message_from_string(send_args[2])
        decoded_parts = [
            p.get_payload(decode=True).decode("utf-8", errors="replace")
            for p in parsed.walk()
            if p.get_content_maintype() == "text"
        ]
        self.assertTrue(any("ACME REALTY LLC" in part for part in decoded_parts))

    def test_review_report_notification_formatting_for_false_positive_missed_link_and_revert(self):
        """Verify /review false_positive, missed_connection, appended note, and revert emails include full context."""
        subj_fp, txt_fp, html_fp = format_review_report_notification(
            report_id=101,
            report_kind="false_positive",
            jurisdiction="ma",
            network_name="SMITH & JONES LAW PC",
            network_key="network_ma_123",
            network_id="123",
            target_type="address",
            target_value="100 FEDERAL ST BOSTON MA",
            target_id="addr_1",
            related_target_type="",
            related_target_value="",
            related_target_id="",
            reason_code="third_party_intermediary",
            explanation="This is an eviction law firm, not the landlord.",
            evidence_url="https://example.org/filing",
            reviewer_label="GBLS Housing Unit",
            source_path="/review/ma?network=network_ma_123",
            bridge_context={"shared_address": "100 FEDERAL ST"},
            deduplicated=False,
        )
        self.assertIn("#101", subj_fp)
        self.assertIn("(MA)", subj_fp)
        self.assertIn("False Positive", subj_fp)
        self.assertIn("GBLS Housing Unit", txt_fp)
        self.assertIn("https://theyownwhat.net/review/ma?network=network_ma_123", txt_fp)
        self.assertIn("This is an eviction law firm", html_fp)
        self.assertIn("https://example.org/filing", html_fp)

        subj_mc, txt_mc, _ = format_review_report_notification(
            report_id=102,
            report_kind="missed_connection",
            jurisdiction="fl",
            network_name="ALPHA HOLDINGS LLC",
            network_key="network_fl_99",
            network_id="99",
            target_type="entity",
            target_value="ALPHA HOLDINGS LLC",
            target_id="ent_a",
            related_target_type="entity",
            related_target_value="BETA APARTMENTS LLC",
            related_target_id="ent_b",
            reason_code="missed_shared_principal",
            explanation="Both LLCs list Jane Doe as manager on Sunbiz.",
            evidence_url="",
            reviewer_label="Florida Legal Services",
            source_path="/review/fl",
            bridge_context={"clue": "same manager"},
            deduplicated=True,
        )
        self.assertIn("Note added to /review report #102 (FL)", subj_mc)
        self.assertIn("BETA APARTMENTS LLC", txt_mc)
        self.assertIn("Related Target (Side B)", txt_mc)

        subj_rev, txt_rev, _ = format_revert_notification(
            {
                "report_id": 101,
                "deactivated_rules": [{"rule_type": "address", "match_value": "100 FEDERAL ST"}],
                "rebuild_jurisdictions": ["ma"],
                "rebuild_status": "queued",
            },
            reverted_by="salmun@storrsmagic.org",
            reason="Overly broad rule",
        )
        self.assertIn("#101", subj_rev)
        self.assertIn("Overly broad rule", txt_rev)
        self.assertIn("MA", txt_rev)

    def test_review_report_post_spawns_email_on_create_and_dedup_append(self):
        """Verify POST /api/review-reports dispatches notification emails for both new reports and appended notes."""
        from fastapi.testclient import TestClient
        from api.main import app

        report_id = None
        with patch("api.reviewer_feedback_routes.spawn_feedback_notification_email") as mock_spawn:
            with TestClient(app) as client:
                base_payload = {
                    "report_kind": "missed_connection",
                    "jurisdiction": "ma",
                    "network_id": "test_net_email_999",
                    "network_key": "network_ma_test_email_999",
                    "network_name": "TEST LANDLORD GROUP LLC",
                    "target_type": "entity",
                    "target_value": "TEST LANDLORD GROUP LLC",
                    "related_target_type": "entity",
                    "related_target_value": "TEST AFFILIATE HOLDINGS LLC",
                    "reason_code": "missed_shared_address",
                    "explanation": "Initial automated test explanation.",
                    "reviewer_label": "Unit Test Reviewer",
                    "source_path": "/review/ma",
                }
                r1 = client.post("/api/review-reports", json=base_payload)
                self.assertEqual(r1.status_code, 200, r1.text)
                d1 = r1.json()
                self.assertTrue(d1["ok"])
                self.assertFalse(d1["deduplicated"])
                report_id = d1["report_id"]

                # Second identical report within 24h appends a note and also emails
                r2 = client.post(
                    "/api/review-reports",
                    json={**base_payload, "explanation": "Second note with additional clue."},
                )
                self.assertEqual(r2.status_code, 200, r2.text)
                d2 = r2.json()
                self.assertTrue(d2["deduplicated"])
                self.assertEqual(d2["report_id"], report_id)

        try:
            self.assertEqual(mock_spawn.call_count, 2)
            first_subj, first_body = mock_spawn.call_args_list[0].args[:2]
            second_subj, second_body = mock_spawn.call_args_list[1].args[:2]
            self.assertIn("New /review Missed Connection", first_subj)
            self.assertIn("TEST AFFILIATE HOLDINGS LLC", first_body)
            self.assertIn("Note added to /review report", second_subj)
            self.assertIn("Second note with additional clue.", second_body)
        finally:
            if report_id is not None:
                with self.conn.cursor() as cur:
                    cur.execute("DELETE FROM network_error_reports WHERE id = %s", (report_id,))
                self.conn.commit()

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

        try:
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
        finally:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM user_feedback WHERE id = %s", (feedback_id,))
            self.conn.commit()

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
        """Verify autocomplete is functional across multiple jurisdictions including MIAMI and MINNEAPOLIS."""
        jurisdictions = [
            ("CT", "hartford"),
            ("NY", "park"),
            ("DC", "street"),
            ("BALTIMORE", "baltimore"),
            ("CHICAGO", "main"),
            ("MIAMI", "flagler"),
            ("MINNEAPOLIS", "hennepin"),
        ]

        for state, term in jurisdictions:
            q = urllib.parse.quote(term)
            url = f"{API_BASE_URL}/api/autocomplete?q={q}&type=all&state={state}"
            with urllib.request.urlopen(url) as resp:
                self.assertEqual(resp.status, 200, f"Autocomplete failed for state={state}")
                results = json.loads(resp.read().decode("utf-8"))
                self.assertIsInstance(results, list, f"Non-list returned for state={state}")

    def test_rap_sheets_monitor_hartford_and_non_ct(self):
        """Verify Hartford Rap Sheets (network, llc, attorney) and non-CT Rap Sheets (NYC, DETROIT)."""
        # 1. Hartford network dimension
        with urllib.request.urlopen(f"{API_BASE_URL}/api/monitor?city=HARTFORD&dimension=network&sort_by=violations") as resp:
            self.assertEqual(resp.status, 200)
            payload = json.loads(resp.read().decode("utf-8"))
            rows = payload.get("results", payload) if isinstance(payload, dict) else payload
            self.assertTrue(len(rows) > 0, "Hartford Rap Sheets returned 0 networks!")
            self.assertGreater(rows[0].get("violation_count", 0), 0)

        # 2. Hartford LLC dimension (must be Hartford-scoped)
        with urllib.request.urlopen(f"{API_BASE_URL}/api/monitor?city=HARTFORD&dimension=llc&sort_by=evictions") as resp:
            self.assertEqual(resp.status, 200)
            payload = json.loads(resp.read().decode("utf-8"))
            rows = payload.get("results", payload) if isinstance(payload, dict) else payload
            self.assertTrue(len(rows) > 0, "Hartford LLC Rap Sheets returned 0 rows!")

        # 3. NYC & Detroit non-CT Rap Sheets exclude government/land-bank entities
        for city in ("NYC", "DETROIT"):
            with urllib.request.urlopen(f"{API_BASE_URL}/api/monitor?city={city}&dimension=network&sort_by=violations") as resp:
                self.assertEqual(resp.status, 200)
                payload = json.loads(resp.read().decode("utf-8"))
                rows = payload.get("results", payload) if isinstance(payload, dict) else payload
                self.assertTrue(len(rows) > 0, f"{city} Rap Sheets returned 0 rows!")
                for r in rows[:20]:
                    name_up = (r.get("entity_name") or r.get("name") or "").upper()
                    self.assertNotIn("LAND BANK", name_up, f"Institutional entity leaked into {city} Rap Sheets: {name_up}")
                    self.assertNotIn("CITY OF NEW YORK", name_up, f"Institutional entity leaked into {city} Rap Sheets: {name_up}")

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

    def test_city_search_initialisms_and_opencorporates_principals(self):
        """Verify initialism queries like 'WC Smith' (DC) and 'DR Horton' (VA) match corporate/principal networks."""
        for city, query, min_bldgs in (
            ("dc", "WC Smith", 50),
            ("va", "DR Horton", 150),
            ("ma", "Dudley Neighbors", 140),
            ("baltimore", "Baltimore SFR", 500),
        ):
            url = f"{API_BASE_URL}/api/{city}/search?q={urllib.parse.quote(query)}&limit=8"
            with urllib.request.urlopen(url) as resp:
                self.assertEqual(resp.status, 200)
                results = json.loads(resp.read().decode("utf-8"))
                net_hits = [r for r in results if str(r.get("type", "")).endswith("_network")]
                self.assertTrue(
                    len(net_hits) > 0,
                    f"Expected network hit for {query!r} in {city}, got: {results[:3]}",
                )
                self.assertGreaterEqual(net_hits[0].get("building_count", 0), min_bldgs)


if __name__ == "__main__":
    unittest.main()
