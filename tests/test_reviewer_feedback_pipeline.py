import os
import sys
import tempfile
import unittest
from urllib.parse import parse_qs, urlparse

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.audit_review_routes import verify_review_token
from api.linking_rules import (
    deactivate_learned_rule,
    reload_learned_rules,
    upsert_learned_rule,
)
from api.reviewer_feedback_routes import build_revert_report_url
from api.shared_utils import (
    get_email_match_key,
    is_excluded_third_party_entity,
    normalize_business_name,
    normalize_person_name,
)
from updater.daily_usage_report import render_reviewer_reports_digest_section
from updater.evaluate_reviewer_reports import (
    discover_missed_connection_tweaks,
    evaluate_address_hub_evidence,
    evaluate_email_hub_evidence,
    evaluate_nominee_manager_evidence,
    evaluate_non_landlord_entity_evidence,
    evaluate_principal_evidence,
)


class ReviewerFeedbackPipelineTests(unittest.TestCase):
    # --- 1. Anti-Weaponization Safeguards (Disgruntled Landlord Protection) ---

    def test_rejects_cohesive_landlord_hq_address(self):
        verdict, rule, rationale = evaluate_address_hub_evidence({
            "coarse_address": "142 OAKWOOD AVENUE",
            "total_businesses": 12,
            "owning_businesses": 11,
            "owning_ratio": 0.917,
            "distinct_owner_families": 1,
            "distinct_unrelated_surnames": 1,
            "distinct_suites": 1,
            "top1_family_share": 0.95,
            "top2_family_share": 1.0,
            "has_intermediary_marker": False,
        })
        self.assertEqual(verdict, "rejected")
        self.assertIsNone(rule)
        self.assertIn("concentrated landlord office", rationale)

    def test_rejects_cohesive_landlord_email(self):
        verdict, rule, _ = evaluate_email_hub_evidence({
            "email": "ops@gurevitchrealty.com",
            "domain": "gurevitchrealty.com",
            "total_businesses": 28,
            "distinct_hqs": 1,
            "top2_hq_share": 1.0,
            "distinct_surnames": 2,
            "top2_surname_share": 1.0,
            "is_professional_intermediary": False,
        })
        self.assertEqual(verdict, "rejected")
        self.assertIsNone(rule)

    def test_rejects_legitimate_concentrated_principal(self):
        verdict, rule, _ = evaluate_principal_evidence({
            "principal_name": "MENACHEM GUREVITCH",
            "total_companies": 45,
            "distinct_hqs": 2,
            "top2_hq_share": 0.92,
            "distinct_families": 4,
            "has_intermediary_role": False,
        })
        self.assertEqual(verdict, "rejected")
        self.assertIsNone(rule)

    def test_rejects_legitimate_private_llc_as_non_landlord(self):
        verdict, rule, _ = evaluate_non_landlord_entity_evidence({
            "entity_name": "ELM STREET APARTMENTS LLC",
            "entity_type": "Domestic Limited Liability Company",
        })
        self.assertEqual(verdict, "rejected")
        self.assertIsNone(rule)

    def test_rejects_cohesive_holding_company_as_nominee_factory(self):
        verdict, rule, _ = evaluate_nominee_manager_evidence({
            "manager_name": "GUREVITCH HOLDINGS LLC",
            "managed_companies": 24,
            "distinct_families": 4,
            "distinct_hqs": 1,
            "top2_hq_share": 1.0,
        })
        self.assertEqual(verdict, "rejected")
        self.assertIsNone(rule)

    # --- 2. Positive Public-Record Verification & Global Generalization ---

    def test_verifies_multi_suite_office_or_ra_address_hub(self):
        verdict, rule, _ = evaluate_address_hub_evidence({
            "coarse_address": "129 CHURCH STREET",
            "total_businesses": 180,
            "owning_businesses": 14,
            "owning_ratio": 0.078,
            "distinct_owner_families": 38,
            "distinct_unrelated_surnames": 19,
            "distinct_suites": 16,
            "top1_family_share": 0.09,
            "top2_family_share": 0.16,
            "has_intermediary_marker": True,
        })
        self.assertEqual(verdict, "verified")
        self.assertEqual(rule["rule_type"], "third_party_hub_block")
        self.assertEqual(rule["rule_key"], "129 CHURCH STREET")
        self.assertEqual(rule["scope"], "global")

    def test_generalizes_law_firm_domain_but_scopes_webmail_to_exact_address(self):
        v1, r1, _ = evaluate_email_hub_evidence({
            "email": "paralegal@smithlawfirm.com",
            "domain": "smithlawfirm.com",
            "total_businesses": 18,
            "distinct_hqs": 9,
            "top2_hq_share": 0.25,
            "distinct_surnames": 10,
            "top2_surname_share": 0.22,
            "is_professional_intermediary": True,
        })
        self.assertEqual(v1, "verified")
        self.assertEqual(r1["rule_type"], "third_party_email_block")
        self.assertEqual(r1["rule_key"], "smithlawfirm.com")

        v2, r2, _ = evaluate_email_hub_evidence({
            "email": "closingattorney99@gmail.com",
            "domain": "gmail.com",
            "total_businesses": 15,
            "distinct_hqs": 8,
            "top2_hq_share": 0.25,
            "distinct_surnames": 9,
            "top2_surname_share": 0.20,
            "is_professional_intermediary": True,
        })
        self.assertEqual(v2, "verified")
        self.assertEqual(r2["rule_type"], "third_party_email_block")
        self.assertEqual(r2["rule_key"], "closingattorney99@gmail.com")

    def test_distinguishes_common_homonym_from_intermediary_principal(self):
        v_hom, r_hom, _ = evaluate_principal_evidence({
            "principal_name": "JAMES CARTER",
            "total_companies": 14,
            "distinct_hqs": 9,
            "top2_hq_share": 0.28,
            "distinct_families": 11,
            "has_intermediary_role": False,
        })
        self.assertEqual(v_hom, "verified")
        self.assertEqual(r_hom["rule_type"], "ambiguous_homonym_block")

        v_att, r_att, _ = evaluate_principal_evidence({
            "principal_name": "MARGARET VANCE",
            "total_companies": 32,
            "distinct_hqs": 18,
            "top2_hq_share": 0.15,
            "distinct_families": 22,
            "has_intermediary_role": True,
        })
        self.assertEqual(v_att, "verified")
        self.assertEqual(r_att["rule_type"], "third_party_principal_block")

    def test_verifies_commercial_nominee_manager_factory(self):
        verdict, rule, _ = evaluate_nominee_manager_evidence({
            "manager_name": "VESTA PROPERTIES MANAGER LLC",
            "managed_companies": 48,
            "distinct_families": 36,
            "distinct_hqs": 24,
            "top2_hq_share": 0.14,
        })
        self.assertEqual(verdict, "verified")
        self.assertEqual(rule["rule_type"], "nominee_manager_block")
        self.assertEqual(rule["scope"], "global")

    def test_verifies_non_landlord_coop_and_placeholder(self):
        v1, r1, _ = evaluate_non_landlord_entity_evidence({
            "entity_name": "OAKRIDGE RESIDENTS MUTUAL HOUSING INC",
            "entity_type": "Non-Stock Nonprofit Cooperative",
        })
        self.assertEqual(v1, "verified")
        self.assertEqual(r1["rule_type"], "non_landlord_entity_block")

        v2, r2, _ = evaluate_non_landlord_entity_evidence({
            "entity_name": "TRUST AGREEMENT DATED",
            "entity_type": "",
        })
        self.assertEqual(v2, "verified")
        self.assertEqual(r2["rule_type"], "placeholder_owner")

    # --- 3. Dynamic Global Rule Application & Deterministic Revert ---

    def test_learned_rules_apply_immediately_and_revert_cleanly(self):
        with tempfile.TemporaryDirectory() as td:
            tmp_json = os.path.join(td, "learned_rules.json")
            try:
                reload_learned_rules(conn=None, json_path=tmp_json)

                # Non-landlord entity rule + @lru_cache invalidation
                self.assertFalse(is_excluded_third_party_entity("SUNRISE RESIDENTS SYNDICATE"))
                upsert_learned_rule(
                    None,
                    "non_landlord_entity_block",
                    "SUNRISE RESIDENTS SYNDICATE",
                    json_path=tmp_json,
                )
                self.assertTrue(is_excluded_third_party_entity("SUNRISE RESIDENTS SYNDICATE"))
                deactivate_learned_rule(
                    None,
                    "non_landlord_entity_block",
                    "SUNRISE RESIDENTS SYNDICATE",
                    json_path=tmp_json,
                )
                self.assertFalse(is_excluded_third_party_entity("SUNRISE RESIDENTS SYNDICATE"))

                # Exact webmail block must NOT block unrelated gmail addresses
                self.assertEqual(
                    get_email_match_key("closingattorney99@gmail.com", {"gmail.com": "public"}),
                    "closingattorney99@gmail.com",
                )
                upsert_learned_rule(
                    None,
                    "third_party_email_block",
                    "closingattorney99@gmail.com",
                    json_path=tmp_json,
                )
                self.assertIsNone(
                    get_email_match_key("closingattorney99@gmail.com", {"gmail.com": "public"})
                )
                self.assertEqual(
                    get_email_match_key("realowner@gmail.com", {"gmail.com": "public"}),
                    "realowner@gmail.com",
                )
                deactivate_learned_rule(
                    None,
                    "third_party_email_block",
                    "closingattorney99@gmail.com",
                    json_path=tmp_json,
                )
                self.assertEqual(
                    get_email_match_key("closingattorney99@gmail.com", {"gmail.com": "public"}),
                    "closingattorney99@gmail.com",
                )
            finally:
                reload_learned_rules(conn=None)

    # --- 4. Daily Digest Formatting + Signed 1-Click Revert Link ---

    def test_digest_renders_verified_and_rejected_reports_with_valid_revert_token(self):
        reports = [
            {
                "id": 42,
                "status": "verified_applied",
                "jurisdiction": "ct",
                "network_name": "Menachem Gurevitch",
                "target_type": "address",
                "target_value": "129 CHURCH STREET",
                "reason_code": "shared_office_or_maildrop",
                "explanation": "Multi-tenant <script>alert(1)</script> office tower",
                "reviewer_label": "Sam",
                "evaluation_summary": {"rationale": "Confirmed multi-suite office building (16 suites)."},
                "applied_rules": [
                    {"rule_type": "third_party_hub_block", "rule_key": "129 CHURCH STREET", "scope": "global"}
                ],
                "affected_jurisdictions": ["ct", "ny"],
                "rebuild_summary": {
                    "before": {"property_count": 1400, "principal_count": 88},
                    "after": {"property_count": 1324, "principal_count": 57},
                },
            },
            {
                "id": 43,
                "status": "rejected_unverified",
                "jurisdiction": "philadelphia",
                "network_name": "Waybar Development LLC",
                "target_type": "principal",
                "target_value": "Eli Alon",
                "reason_code": "wrong_person",
                "explanation": "Remove me please",
                "reviewer_label": "",
                "evaluation_summary": {"rationale": "No linking change applied — cohesive single-HQ principal."},
                "applied_rules": [],
                "affected_jurisdictions": [],
                "rebuild_summary": {},
            },
        ]
        text_out, html_out = render_reviewer_reports_digest_section(
            reports, base_url="https://theyownwhat.net"
        )
        self.assertIn("VERIFIED & APPLIED", text_out)
        self.assertIn("REJECTED — LINK VALID", text_out)
        self.assertIn("Properties: 1400 -> 1324 | Principals: 88 -> 57", text_out)
        self.assertNotIn("<script>", html_out)
        self.assertIn("&lt;script&gt;", html_out)
        self.assertIn("Revert This Change", html_out)

        # Verify HMAC signature embedded in generated revert URL
        revert_url = build_revert_report_url("https://theyownwhat.net", 42)
        parsed = urlparse(revert_url)
        qs = parse_qs(parsed.query)
        self.assertEqual(qs["report_id"][0], "42")
        ok, payload, err = verify_review_token(qs["token"][0], expected_item_key="review-report:42")
        self.assertTrue(ok, err)
        self.assertEqual(payload["k"], "review-report:42")

    # --- 5. Missed-Connection Organic Builder Tweaks & Anti-Fabrication ---

    def test_missed_connection_discovers_overblocked_cohesive_hq_ra_allowlist(self):
        out = discover_missed_connection_tweaks(
            side_a={"entities": ["OAKWOOD HOLDINGS LLC"], "raw_addresses": ["142 OAKWOOD AVE, HARTFORD, CT 06106"]},
            side_b={"entities": ["OAKWOOD ANNEX LLC"], "raw_addresses": ["142 OAKWOOD AVENUE, HARTFORD, CT 06106"]},
            blocked_addresses=["142 OAKWOOD AVENUE"],
            address_stats_fn=lambda _: {
                "coarse_address": "142 OAKWOOD AVENUE",
                "owning_ratio": 0.90,
                "distinct_owner_families": 2,
                "distinct_suites": 1,
                "top1_family_share": 0.85,
                "has_intermediary_marker": False,
            },
        )
        self.assertEqual(out["status"], "verified_applied")
        self.assertTrue(any(r["rule_type"] == "ra_allowlist" and r["rule_key"] == "142 OAKWOOD AVENUE" for r in out["rules"]))

    def test_missed_connection_refuses_to_allowlist_genuine_ra_or_ups_hub(self):
        out = discover_missed_connection_tweaks(
            side_a={"entities": ["ALPHA LLC"], "raw_addresses": ["129 CHURCH ST, NEW HAVEN, CT 06510"]},
            side_b={"entities": ["BETA LLC"], "raw_addresses": ["129 CHURCH STREET, NEW HAVEN, CT 06510"]},
            blocked_addresses=["129 CHURCH STREET"],
            address_stats_fn=lambda _: {
                "coarse_address": "129 CHURCH STREET",
                "owning_ratio": 0.08,
                "distinct_owner_families": 42,
                "distinct_suites": 18,
                "top1_family_share": 0.09,
                "has_intermediary_marker": True,
            },
        )
        self.assertEqual(out["status"], "rejected_unverified")
        self.assertEqual(out["rules"], [])

    def test_missed_connection_discovers_benign_care_of_token_for_internal_operator(self):
        out = discover_missed_connection_tweaks(
            side_a={"entities": ["GUREVITCH HOLDINGS LLC"], "raw_addresses": ["142 OAKWOOD AVE, HARTFORD, CT 06106"]},
            side_b={"entities": ["ELM STREET DE LLC"], "raw_addresses": ["C/O GUREVITCH OPS, 142 OAKWOOD AVE, HARTFORD, CT 06106"]},
        )
        self.assertEqual(out["status"], "verified_applied")
        self.assertTrue(any(r["rule_type"] == "benign_care_of_token" and r["rule_key"] == "GUREVITCH OPS" for r in out["rules"]))

    def test_missed_connection_discovers_zip_scoped_address_alias_and_blocks_quadrant_collision(self):
        out_ok = discover_missed_connection_tweaks(
            side_a={"entities": ["MAPLE LLC"], "raw_addresses": ["250 WHALLEY AVE, NEW HAVEN, CT 06511"]},
            side_b={"entities": ["MAPLE II LLC"], "raw_addresses": ["250 WHAALEY AVE, NEW HAVEN, CT 06511"]},
        )
        self.assertEqual(out_ok["status"], "verified_applied")
        self.assertTrue(any(r["rule_type"] == "address_alias" and r["rule_key"].endswith("||06511") for r in out_ok["rules"]))

        # Must NEVER alias different quadrants (NW vs NE) or different lettered avenues (E vs F)!
        out_quad = discover_missed_connection_tweaks(
            side_a={"entities": ["CAPITOL A LLC"], "raw_addresses": ["1200 K ST NW, WASHINGTON, DC 20005"]},
            side_b={"entities": ["CAPITOL B LLC"], "raw_addresses": ["1200 K ST NE, WASHINGTON, DC 20005"]},
        )
        self.assertEqual(out_quad["rules"], [])

    def test_missed_connection_discovers_biz_abbrev_and_officer_role_suffix(self):
        out_abbr = discover_missed_connection_tweaks(
            side_a={"entities": ["HARBORVIEW MNGMNT LLC"], "raw_addresses": ["10 MAIN ST, BOSTON, MA 02108"]},
            side_b={"entities": ["HARBORVIEW MANAGEMENT LLC"], "raw_addresses": ["99 STATE ST, BOSTON, MA 02108"]},
        )
        self.assertEqual(out_abbr["status"], "verified_applied")
        self.assertTrue(any(r["rule_type"] == "biz_abbrev" and r["rule_key"] == "MNGMNT" and r["rule_value"] == "MANAGEMENT" for r in out_abbr["rules"]))

        out_role = discover_missed_connection_tweaks(
            side_a={"entities": ["ALPHA LLC"], "principals": ["SARAH CHEN"]},
            side_b={"entities": ["BETA LLC"], "principals": ["SARAH CHEN MGRMEM"]},
        )
        self.assertEqual(out_role["status"], "verified_applied")
        self.assertTrue(any(r["rule_type"] == "officer_role_suffix" and r["rule_key"] == "MGRMEM" for r in out_role["rules"]))

    def test_missed_connection_discovers_false_positive_entity_allowlist_for_for_profit_llc(self):
        # 'UNIVERSITY' keyword triggers THIRD_PARTY_ENTITY_RE on a private LLC name without street suffix
        out = discover_missed_connection_tweaks(
            side_a={
                "entities": ["ELM APARTMENTS LLC"],
                "raw_addresses": ["400 CHAPEL ST, NEW HAVEN, CT 06511"],
            },
            side_b={
                "entities": ["UNIVERSITY COMMONS APARTMENTS LLC"],
                "entity_types": {"UNIVERSITY COMMONS APARTMENTS LLC": "Domestic Limited Liability Company"},
                "raw_addresses": ["400 CHAPEL ST, NEW HAVEN, CT 06511"],
            },
        )
        self.assertEqual(out["status"], "verified_applied")
        self.assertTrue(any(r["rule_type"] == "entity_allowlist" for r in out["rules"]))

    def test_missed_connection_never_hardcodes_merge_for_uncorroborated_sides(self):
        out = discover_missed_connection_tweaks(
            side_a={"entities": ["ALPHA REALTY LLC"], "raw_addresses": ["10 OAK ST, HARTFORD, CT 06106"], "principals": ["ALICE JONES"]},
            side_b={"entities": ["OMEGA TOWERS LLC"], "raw_addresses": ["999 PINE RD, STAMFORD, CT 06901"], "principals": ["BOB WILSON"]},
            has_external_evidence_url=False,
        )
        self.assertEqual(out["status"], "rejected_unverified")
        self.assertEqual(out["rules"], [])

    def test_new_organic_allowlist_and_role_rules_apply_and_revert_cleanly(self):
        with tempfile.TemporaryDirectory() as td:
            tmp_json = os.path.join(td, "learned_rules.json")
            try:
                reload_learned_rules(conn=None, json_path=tmp_json)

                # entity_allowlist overrides false-positive institutional hit & reverts cleanly
                self.assertTrue(is_excluded_third_party_entity("UNIVERSITY COMMONS APARTMENTS LLC"))
                upsert_learned_rule(None, "entity_allowlist", "UNIVERSITY COMMONS APARTMENTS LLC", json_path=tmp_json)
                self.assertFalse(is_excluded_third_party_entity("UNIVERSITY COMMONS APARTMENTS LLC"))
                deactivate_learned_rule(None, "entity_allowlist", "UNIVERSITY COMMONS APARTMENTS LLC", json_path=tmp_json)
                self.assertTrue(is_excluded_third_party_entity("UNIVERSITY COMMONS APARTMENTS LLC"))

                # officer_role_suffix strips in normalize_person_name globally & reverts cleanly
                self.assertEqual(normalize_person_name("SARAH CHEN MGRMEM"), "SARAH CHEN MGRMEM")
                upsert_learned_rule(None, "officer_role_suffix", "MGRMEM", json_path=tmp_json)
                self.assertEqual(normalize_person_name("SARAH CHEN MGRMEM"), "SARAH CHEN")
                deactivate_learned_rule(None, "officer_role_suffix", "MGRMEM", json_path=tmp_json)
                self.assertEqual(normalize_person_name("SARAH CHEN MGRMEM"), "SARAH CHEN MGRMEM")

                # email_allowlist overrides overbroad regex & reverts cleanly
                upsert_learned_rule(None, "email_allowlist", "legalresidential.com", json_path=tmp_json)
                self.assertEqual(get_email_match_key("ops@legalresidential.com", {}), "legalresidential.com")
                deactivate_learned_rule(None, "email_allowlist", "legalresidential.com", json_path=tmp_json)
            finally:
                reload_learned_rules(conn=None)

    def test_digest_renders_missed_connection_report_with_revert_link(self):
        reports = [
            {
                "id": 77,
                "report_kind": "missed_connection",
                "status": "verified_applied",
                "jurisdiction": "ma",
                "network_name": "Harborview Holdings LLC",
                "target_type": "network",
                "target_value": "Harborview Holdings LLC",
                "related_target_type": "entity",
                "related_target_value": "Harborview Hldgs LLC",
                "reason_code": "missed_corporate_affiliate",
                "explanation": "Same operator, abbreviated HLDGS on deed",
                "reviewer_label": "Sam",
                "evaluation_summary": {"rationale": "Adding global `biz_abbrev` rule HLDGS -> HOLDINGS."},
                "applied_rules": [
                    {"rule_type": "biz_abbrev", "rule_key": "HLDGS", "rule_value": "HOLDINGS", "scope": "global"}
                ],
                "affected_jurisdictions": ["ma", "ct"],
                "rebuild_summary": {
                    "before": {"property_count": 42},
                    "after": {"property_count": 51},
                },
            }
        ]
        txt, htm = render_reviewer_reports_digest_section(reports, base_url="https://theyownwhat.net")
        self.assertIn("[MISSED CONNECTION]", txt)
        self.assertIn("Harborview Holdings LLC <-> Harborview Hldgs LLC", txt)
        self.assertIn("MISSED CONNECTION", htm)
        self.assertIn("Revert This Change", htm)


if __name__ == "__main__":
    unittest.main()
