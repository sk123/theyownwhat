#!/usr/bin/env python3
"""
tests/test_weekly_gemini_audit_and_review.py
============================================
Unit and integration tests for:
  1. Dynamic Gemini model discovery (`discover_latest_gemini_model`, `score_gemini_model_candidate`)
  2. Cross-environment HMAC-SHA256 signing & verification (`get_review_signing_secret`,
     `sign_review_token`, `verify_review_token`)
  3. 1-click email review state machine (`record_review_decision`, `resolve_fix_queue_item`,
     `load_review_state`, `filter_unapproved_review_items`)
  4. HTML/text weekly audit digest generation (`build_weekly_audit_email`)
  5. HTTP GET prefetcher safety & strict CSP headers on `/api/audit-review/quick-decision`
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.audit_review_routes import (
    CSP_HEADER,
    get_review_signing_secret,
    load_review_state,
    record_review_decision,
    resolve_fix_queue_item,
    router as audit_review_router,
    sign_review_token,
    verify_review_token,
)
from scripts.weekly_gemini_audit import (
    build_weekly_audit_email,
    discover_latest_gemini_model,
    filter_unapproved_review_items,
    score_gemini_model_candidate,
)


class TestDynamicGeminiModelDiscovery(unittest.TestCase):
    def test_selects_newest_pro_and_excludes_specialized_models(self):
        fake_catalog = {
            "models": [
                {
                    "name": "models/gemini-2.5-pro",
                    "supportedGenerationMethods": ["generateContent", "countTokens"],
                },
                {
                    "name": "models/gemini-3.1-pro-preview",
                    "supportedGenerationMethods": ["generateContent", "countTokens"],
                },
                {
                    "name": "models/gemini-3.1-pro-tts",
                    "supportedGenerationMethods": ["generateContent"],
                },
                {
                    "name": "models/gemini-4.0-pro-image-generation",
                    "supportedGenerationMethods": ["generateContent"],
                },
                {
                    "name": "models/gemini-3.1-flash",
                    "supportedGenerationMethods": ["generateContent"],
                },
                {
                    "name": "models/gemini-3.1-flash-lite",
                    "supportedGenerationMethods": ["generateContent"],
                },
                {
                    "name": "models/text-embedding-004",
                    "supportedGenerationMethods": ["embedContent"],
                },
            ]
        }

        chosen_pro = discover_latest_gemini_model(
            "fake-key",
            tier="pro",
            fetch_json=lambda _url, _hdrs: fake_catalog,
        )
        self.assertEqual(chosen_pro, "gemini-3.1-pro-preview")

        chosen_flash = discover_latest_gemini_model(
            "fake-key",
            tier="flash",
            fetch_json=lambda _url, _hdrs: fake_catalog,
        )
        self.assertEqual(chosen_flash, "gemini-3.1-flash")

        chosen_lite = discover_latest_gemini_model(
            "fake-key",
            tier="flash-lite",
            fetch_json=lambda _url, _hdrs: fake_catalog,
        )
        self.assertEqual(chosen_lite, "gemini-3.1-flash-lite")

    def test_ga_outranks_preview_and_future_major_version_wins_automatically(self):
        ga_31 = score_gemini_model_candidate(
            {"name": "models/gemini-3.1-pro", "supportedGenerationMethods": ["generateContent"]},
            desired_tier="pro",
        )
        prev_31 = score_gemini_model_candidate(
            {"name": "models/gemini-3.1-pro-preview", "supportedGenerationMethods": ["generateContent"]},
            desired_tier="pro",
        )
        prev_40 = score_gemini_model_candidate(
            {"name": "models/gemini-4.0-pro-preview", "supportedGenerationMethods": ["generateContent"]},
            desired_tier="pro",
        )
        self.assertIsNotNone(ga_31)
        self.assertIsNotNone(prev_31)
        self.assertIsNotNone(prev_40)
        self.assertGreater(ga_31, prev_31)
        self.assertGreater(prev_40, ga_31)


class TestReviewTokenAndStateMachine(unittest.TestCase):
    def test_mail_secret_whitespace_parity_between_server_and_github_actions(self):
        server_secret = get_review_signing_secret(
            {"ALERT_EMAIL_PASS": "abcd efgh ijkl mnop", "GEMINI_KEY": "other-local-key"}
        )
        gha_secret = get_review_signing_secret(
            {"MAIL_PASSWORD": "abcdefghijklmnop"}
        )
        self.assertEqual(server_secret, gha_secret)

        tok = sign_review_token("nc-cabarrus-limitation", now_ts=1_800_000_000, ttl_days=14, secret=gha_secret)
        ok, payload, err = verify_review_token(
            tok,
            expected_item_key="nc-cabarrus-limitation",
            now_ts=1_800_100_000,
            secret=server_secret,
        )
        self.assertTrue(ok, err)
        self.assertEqual(payload["k"], "nc-cabarrus-limitation")

        # Mismatched item_key rejected
        ok_wrong, _, err_wrong = verify_review_token(
            tok,
            expected_item_key="different-item",
            now_ts=1_800_100_000,
            secret=server_secret,
        )
        self.assertFalse(ok_wrong)
        self.assertIn("does not match", err_wrong)

        # Expired token rejected
        ok_exp, _, err_exp = verify_review_token(
            tok,
            expected_item_key="nc-cabarrus-limitation",
            now_ts=1_800_000_000 + 20 * 86400,
            secret=server_secret,
        )
        self.assertFalse(ok_exp)
        self.assertIn("expired", err_exp)

        # Tampered token rejected
        ok_bad, _, _ = verify_review_token(
            tok[:-3] + "xyz",
            expected_item_key="nc-cabarrus-limitation",
            now_ts=1_800_100_000,
            secret=server_secret,
        )
        self.assertFalse(ok_bad)

    @patch("api.audit_review_routes._persist_db_decision_best_effort")
    def test_approve_reject_undo_and_resolve_fix_workflow(self, _mock_db):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            dec_path = tmp / "weekly-review-decisions.json"
            fix_path = tmp / "review-fix-queue.json"
            aud_path = tmp / "latest-weekly-gemini-audit.json"

            aud_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-11-15T21:00:00Z",
                        "gemini_model": "gemini-3.1-pro-preview",
                        "headline": "All 18 jurisdictions verified",
                        "review_items": [
                            {
                                "item_key": "nc-cabarrus-geometry-only",
                                "title": "Confirm NC Cabarrus geometry-only parcel limitation copy",
                                "domain": "Limitations Copy",
                                "jurisdiction": "NC",
                            },
                            {
                                "item_key": "fl-dbpr-crosswalk",
                                "title": "Confirm FL DBPR Ch. 509 license link threshold",
                                "domain": "Untangling",
                                "jurisdiction": "FL",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            # 1. Approve first item -> suppressed from future unapproved review items
            res_app = record_review_decision(
                decision="approve",
                item_key="nc-cabarrus-geometry-only",
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            self.assertTrue(res_app["ok"])

            state = load_review_state(
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            remaining = filter_unapproved_review_items(
                state["latestAudit"]["reviewItems"],
                state["decisions"],
            )
            self.assertEqual([r["item_key"] for r in remaining], ["fl-dbpr-crosswalk"])

            # 2. Reject first item -> removes prior approval and opens fix-queue item
            res_rej = record_review_decision(
                decision="reject",
                item_key="nc-cabarrus-geometry-only",
                preset_reasons=["Inaccurate limitations or UI copy"],
                explanation="Mention Cabarrus County explicitly on the NC card.",
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            self.assertTrue(res_rej["ok"])
            fix_id = res_rej["fixRecord"]["id"]

            state_after_rej = load_review_state(
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            self.assertEqual(len(state_after_rej["decisions"]), 0)
            self.assertEqual(len(state_after_rej["fixQueue"]["openItems"]), 1)
            self.assertIn("Cabarrus County", state_after_rej["fixQueue"]["openItems"][0]["explanation"])

            # 3. Automated remediation marks fix RESOLVED
            resolved = resolve_fix_queue_item(
                fix_id,
                "Updated NC card limitation copy and verified build.",
                fix_queue_path=fix_path,
            )
            self.assertEqual(resolved["status"], "RESOLVED")
            state_resolved = load_review_state(
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            self.assertEqual(len(state_resolved["fixQueue"]["openItems"]), 0)
            self.assertEqual(len(state_resolved["fixQueue"]["resolvedItems"]), 1)

            # 4. Undo removes fix entry when requested immediately by user
            res_undo = record_review_decision(
                decision="undo",
                item_key="nc-cabarrus-geometry-only",
                undo_descriptor=res_rej["undoDescriptor"],
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            self.assertTrue(res_undo["ok"])
            state_undone = load_review_state(
                decisions_path=dec_path,
                fix_queue_path=fix_path,
                latest_audit_path=aud_path,
            )
            self.assertEqual(len(state_undone["fixQueue"]["allItems"]), 0)

    def test_http_get_is_safe_and_enforces_strict_csp(self):
        app = FastAPI()
        app.include_router(audit_review_router)
        client = TestClient(app)

        secret = get_review_signing_secret()
        token = sign_review_token("sample-audit-item", secret=secret)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            dec_path = tmp / "decisions.json"
            fix_path = tmp / "fixes.json"

            with patch("api.audit_review_routes.DEFAULT_DECISIONS_PATH", dec_path), patch(
                "api.audit_review_routes.DEFAULT_FIX_QUEUE_PATH", fix_path
            ):
                resp = client.get(
                    "/api/audit-review/quick-decision",
                    params={
                        "decision": "approve",
                        "item_key": "sample-audit-item",
                        "title": "Sample Weekly Audit Check",
                        "token": token,
                    },
                )
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.headers.get("content-security-policy"), CSP_HEADER)
                self.assertIn("Sample Weekly Audit Check", resp.text)
                self.assertNotIn("onclick=", resp.text)
                # GET must NEVER mutate state (safe against email link prefetchers)
                self.assertFalse(dec_path.exists())
                self.assertFalse(fix_path.exists())

    def test_build_weekly_audit_email_includes_signed_approve_and_decline_links(self):
        sample_report = {
            "generated_at": "2026-11-15T21:00:00Z",
            "gemini_model": "gemini-3.1-pro-preview",
            "overall_health": "PASS",
            "headline": "18 live jurisdictions verified with 0 third-party network leaks",
            "executive_summary": "All statewide cadastres and corporate registries passed source-only checks.",
            "findings": [
                {
                    "severity": "info",
                    "jurisdiction": "MA",
                    "title": "All 351 Massachusetts municipalities live",
                    "detail": "2,237,736 deduplicated parcels clustered into 1,630,564 private landlord networks.",
                }
            ],
            "optimization_highlights": ["Refreshed completeness cache across all 18 jurisdictions."],
            "review_items": [
                {
                    "item_key": "nys-non-participating-counties-copy",
                    "title": "Verify NYS 33-county public cadastre limitation notice",
                    "domain": "Limitations Copy",
                    "jurisdiction": "NYS",
                    "why_review_needed": "24 non-NYC counties withhold bulk public parcel extracts under NYS ITS opt-in rules.",
                    "recommendation": "Approve current disclosure on the New York State landing card.",
                }
            ],
            "snapshot": {
                "live_jurisdiction_count": 18,
                "total_properties": 14_500_000,
                "total_networks": 8_900_000,
                "total_third_party_leaks": 0,
                "quarantined_registered_agent_hubs": 12500,
                "jurisdictions": [
                    {
                        "code": "MA",
                        "label": "Massachusetts (351 Towns)",
                        "properties": 2237736,
                        "networks": 1630564,
                        "max_network_size": 412,
                        "third_party_leaks": 0,
                    }
                ],
                "recent_commits": [
                    {
                        "short_sha": "4994e54",
                        "subject": "Expand statewide coverage and add weekly Gemini audit",
                    }
                ],
                "open_fix_items": [],
            },
        }
        subj, plain, html_body = build_weekly_audit_email(
            sample_report,
            public_base_url="https://theyownwhat.net",
        )
        self.assertIn("They Own WHAT??", subj)
        self.assertIn("https://theyownwhat.net/api/audit-review/quick-decision?", html_body)
        self.assertIn("decision=approve", html_body)
        self.assertIn("decision=reject", html_body)
        self.assertIn("nys-non-participating-counties-copy", html_body)
        self.assertIn("gemini-3.1-pro-preview", html_body)
        self.assertIn("nys-non-participating-counties-copy", plain)


if __name__ == "__main__":
    unittest.main()
