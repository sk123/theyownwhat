import json
import os
import tempfile
import unittest

from api import linking_rules
from api.oc_linker import clean_oc_officer_name
from api.populate_cities import _is_single_generic_owner_token
from api.shared_utils import (
    clear_shared_utils_caches,
    extract_postal_street_zip_key,
    is_excluded_third_party_address,
    normalize_business_name,
    normalize_mailing_address_coarse,
    strip_benign_care_of_prefix,
)
from updater.nightly_network_optimizer import (
    ALL_JURISDICTIONS,
    DEFAULT_REPORT_RECIPIENT,
    discover_best_gemini_models,
    render_nightly_report,
    score_gemini_model_candidate,
    select_tonight_cohort,
)


class TestNightlyNetworkOptimizer(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._rules_path = os.path.join(self._tmpdir.name, "learned_linking_rules.json")
        linking_rules.reload_learned_rules(conn=None, json_path=self._rules_path)
        clear_shared_utils_caches()

    def tearDown(self):
        linking_rules.reload_learned_rules(conn=None)
        clear_shared_utils_caches()
        self._tmpdir.cleanup()

    def test_five_jurisdiction_rotation_covers_all_20_in_4_nights(self):
        state = {"cursor_index": 0, "cycle_number": 1, "priority_queue": []}
        seen = []
        for night in range(4):
            cohort, next_cur, next_cyc = select_tonight_cohort(state, cohort_size=5)
            self.assertEqual(len(cohort), 5)
            self.assertEqual(len(set(cohort)), 5)
            seen.extend(cohort)
            state["cursor_index"] = next_cur
            state["cycle_number"] = next_cyc
        self.assertEqual(len(seen), 20)
        self.assertEqual(set(seen), set(ALL_JURISDICTIONS))
        self.assertEqual(state["cycle_number"], 2)
        self.assertEqual(state["cursor_index"], 0)

    def test_priority_queue_pulls_one_without_stalling_rotation(self):
        state = {"cursor_index": 0, "cycle_number": 1, "priority_queue": ["vt", "dc"]}
        cohort, next_cur, _ = select_tonight_cohort(state, cohort_size=5)
        self.assertEqual(len(cohort), 5)
        self.assertEqual(cohort[0], "vt")
        self.assertEqual(state["priority_queue"], ["dc"])
        self.assertEqual(next_cur, 4)

    def test_best_available_gemini_model_discovery_prefers_newest_generation(self):
        fake_catalog = {
            "models": [
                {"name": "models/gemini-2.5-pro", "supportedGenerationMethods": ["generateContent"], "thinking": True, "outputTokenLimit": 65536},
                {"name": "models/gemini-3.1-pro-preview", "supportedGenerationMethods": ["generateContent"], "thinking": True, "outputTokenLimit": 65536},
                {"name": "models/gemini-3.8-flash", "supportedGenerationMethods": ["generateContent"], "thinking": True, "outputTokenLimit": 65536},
                {"name": "models/gemini-4-argon", "supportedGenerationMethods": ["generateContent"], "thinking": True, "outputTokenLimit": 32768},
                {"name": "models/gemini-4-hydrogen", "supportedGenerationMethods": ["generateContent"], "thinking": True, "outputTokenLimit": 65536},
                {"name": "models/gemini-4-neon", "supportedGenerationMethods": ["generateContent"], "thinking": True, "outputTokenLimit": 65536},
                {"name": "models/gemini-4.0-pro-tts", "supportedGenerationMethods": ["generateContent"]},
                {"name": "models/text-embedding-004", "supportedGenerationMethods": ["embedContent"]},
            ]
        }
        primary, fallback = discover_best_gemini_models(
            "fake-key", fetch_json=lambda url, hdrs: fake_catalog
        )
        self.assertEqual(primary, "gemini-4-neon")
        self.assertEqual(fallback, "gemini-4-hydrogen")
        self.assertIsNone(
            score_gemini_model_candidate(
                {"name": "models/gemini-4.0-pro-tts", "supportedGenerationMethods": ["generateContent"]},
                "best",
            )
        )

    def test_learned_linking_rules_propagate_to_shared_engine_and_rollback_cleanly(self):
        # 1. Learned business abbreviation
        linking_rules.upsert_learned_rule(
            None, "biz_abbrev", "INVST", "INVESTMENTS",
            scope="global", discovered_in="ma", json_path=self._rules_path,
        )
        clear_shared_utils_caches()
        self.assertEqual(normalize_business_name("OAKWOOD INVST LLC"), "OAKWOOD INVESTMENTS LLC")

        # 2. Learned placeholder owner
        linking_rules.upsert_learned_rule(
            None, "placeholder_owner", "PRIVACY SHIELDED OWNER", "EXCLUDE",
            scope="global", discovered_in="ny", json_path=self._rules_path,
        )
        self.assertTrue(_is_single_generic_owner_token("PRIVACY SHIELDED OWNER"))

        # 3. Learned ZIP-scoped address alias (strictly within one ZIP, never cross-state)
        linking_rules.upsert_learned_rule(
            None, "address_alias", "60 LEO M BIRMINGHAM PARKWAY||02135", "60 LEO BIRMINGHAM PARKWAY||02135",
            scope="ma", discovered_in="ma", json_path=self._rules_path,
        )
        self.assertEqual(
            extract_postal_street_zip_key("60 LEO M BIRMINGHAM PKWY, BRIGHTON, MA 02135"),
            "60 LEO BIRMINGHAM PARKWAY||02135",
        )

        # 4. Learned officer role suffix
        linking_rules.upsert_learned_rule(
            None, "officer_role_suffix", "AUTH SIGNER", "STRIP",
            scope="global", discovered_in="fl", json_path=self._rules_path,
        )
        self.assertEqual(clean_oc_officer_name("LEONARD SAMIA AUTH SIGNER"), "LEONARD SAMIA")

        # 5. Rollback via deactivate_learned_rule restores prior behavior
        linking_rules.deactivate_learned_rule(
            None, "biz_abbrev", "INVST", scope="global", json_path=self._rules_path,
        )
        clear_shared_utils_caches()
        self.assertEqual(normalize_business_name("OAKWOOD INVST LLC"), "OAKWOOD INVST LLC")

    def test_nightly_report_rendering_and_default_recipient(self):
        self.assertEqual(DEFAULT_REPORT_RECIPIENT, "salmun@storrsmagic.org")
        summary = {
            "run_id": "nlo-20261119T051500Z",
            "started_at": "2026-11-19T05:15:00Z",
            "dry_run": False,
            "cycle_number": 1,
            "next_cursor_index": 5,
            "gemini_primary_model": "gemini-2.5-pro",
            "gemini_fallback_model": "gemini-2.5-flash",
            "total_elapsed_s": 142.3,
            "peak_rss_mb": 412.5,
            "propagated_rules": [
                {"rule_type": "biz_abbrev", "rule_key": "INVST", "rule_value": "INVESTMENTS", "scope": "global", "discovered_in": "ma"}
            ],
            "propagation_queued_jurisdictions": ["fl"],
            "results": [
                {
                    "jurisdiction": "ma",
                    "label": "Massachusetts (351 Towns)",
                    "status": "optimized",
                    "rebuilt": True,
                    "elapsed_s": 42.1,
                    "initial_metrics": {"total_parcels": 2248876, "total_networks": 1681236, "multi_entity_networks": 41000},
                    "final_metrics": {"total_parcels": 2248876, "total_networks": 1680910, "multi_entity_networks": 41250},
                    "passes": [
                        {
                            "pass_number": 1,
                            "status": "applied_and_rebuilt",
                            "examine_s": 8.4,
                            "findings_summary": {"cohesive_hqs": 1, "placeholders": 0},
                            "rebuild_timings": {"total_s": 31.2},
                        },
                        {
                            "pass_number": 2,
                            "status": "converged",
                            "examine_s": 7.9,
                            "findings_summary": {"cohesive_hqs": 0, "placeholders": 0},
                            "rebuild_timings": {},
                        },
                    ],
                    "applied_rules": [
                        {"rule_type": "biz_abbrev", "rule_key": "INVST", "rule_value": "INVESTMENTS", "scope": "global"}
                    ],
                    "perf_audit": {"actions_taken": ["ANALYZE ma_networks (elapsed=0.42s)"], "recommendations": []},
                    "gemini_review": {
                        "ok": True,
                        "model_used": "gemini-2.5-pro",
                        "advisory": {
                            "jurisdiction_assessment": "MA portfolios show strong HQ cohesion with zero hairballs.",
                            "missed_connection_hypotheses": [],
                            "false_positive_risks": [],
                        },
                    },
                }
            ],
        }
        subj, text, html_body, md = render_nightly_report(summary)
        self.assertIn("MA", subj)
        self.assertIn("1 rule(s)", subj)
        self.assertIn("gemini-2.5-pro", text)
        self.assertIn("strictly jurisdiction-by-jurisdiction", md)
        self.assertIn("ANALYZE ma_networks", html_body)


if __name__ == "__main__":
    unittest.main()
