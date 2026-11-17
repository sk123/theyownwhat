#!/usr/bin/env python3
"""
tests/test_jurisdiction_rotation_engine.py
==========================================
Unit and regression tests for:
  - Ledger-driven jurisdiction rotation engine (`scripts/rotate_jurisdiction_optimization.py`),
    including Cycle 1 `nightly` -> Cycle 2 `weekly` automatic cadence rollover and strict
    evidence validation.
  - CT Geodata CSV column normalization (`_normalize_ct_geodata_columns` in `updater/update_data.py`),
    covering UTF-8 BOM headers, duplicate `OBJECTID`/`Location` headers, and space-vs-underscore
    schema variants (`EAST HARTFORD`, `GREENWICH`, `GROTON`).
  - Multi-format source date parsing (`parse_source_date` in `updater/enrich_city_enforcement.py`),
    including US slash timestamps (`10/1/2025 12:00:00 AM`) in DC DOB violation CSVs.
  - NY DOS Active Corporations commercial-agent & entity-vs-human guards (`nyc/enrich_ny_dos.py`).
"""

import copy
import datetime as dt
import unittest

import pandas as pd

from nyc.enrich_ny_dos import clean_str, is_commercial_agent_name
from scripts.rotate_jurisdiction_optimization import (
    DEFAULT_MANIFEST_PATH,
    advance_manifest,
    compute_status,
    format_iso_utc,
    load_manifest,
    validate_manifest,
)
from updater.enrich_city_enforcement import parse_source_date
from updater.update_data import _normalize_ct_geodata_columns


class TestJurisdictionRotationLedger(unittest.TestCase):
    def test_live_manifest_is_valid_and_covers_all_cohorts(self):
        manifest = load_manifest(DEFAULT_MANIFEST_PATH)
        errors = validate_manifest(manifest)
        self.assertEqual(errors, [], f"Live manifest failed validation: {errors}")
        self.assertEqual(len(manifest["cohorts"]), 9)

        codes = [
            j["code"]
            for cohort in manifest["cohorts"]
            for j in cohort["jurisdictions"]
        ]
        for expected in (
            "CT",
            "NYC",
            "NJ",
            "DC",
            "BALTIMORE",
            "BOSTON",
            "DETROIT",
            "MIAMI",
            "MINNEAPOLIS",
            "PHILADELPHIA",
            "CHICAGO",
            "MA",
            "FL",
            "NYS",
            "CO",
            "NC",
            "VA",
            "AR",
            "WI",
            "VT",
            "CT_MUNI_COHORT_A",
            "CT_MUNI_COHORT_B",
        ):
            self.assertIn(expected, codes)

    def test_cycle_1_nightly_rolls_over_to_cycle_2_weekly(self):
        manifest = copy.deepcopy(load_manifest(DEFAULT_MANIFEST_PATH))
        manifest["currentCycle"] = 1
        manifest["nextCohortIndex"] = 0
        manifest["lastRunAt"] = None
        manifest["lastAdvancedAt"] = None
        manifest["completedJurisdictionsInCycle"] = []
        manifest["history"] = []

        base_now = dt.datetime(2026, 11, 15, 7, 0, tzinfo=dt.timezone.utc)
        initial_status = compute_status(manifest, now=base_now)
        self.assertTrue(initial_status["isDue"])
        self.assertEqual(initial_status["cadencePhase"], "initialCoverage")
        self.assertEqual(initial_status["frequency"], "nightly")

        # Advance all cohorts at 24-hour intervals
        total_cohorts = len(manifest["cohorts"])
        cur_manifest = manifest
        for step in range(total_cohorts):
            now_step = base_now + dt.timedelta(days=step)
            cohort = cur_manifest["cohorts"][cur_manifest["nextCohortIndex"]]
            evidence = {
                "cohort_id": cohort["id"],
                "overall_status": "PASS",
                "all_checks_passed": True,
                "total_checks": 24,
                "passed_checks": 24,
                "failed_checks": 0,
                "jurisdictions_verified": [j["code"] for j in cohort["jurisdictions"]],
                "healed_feeds": [],
            }
            cur_manifest, entry = advance_manifest(
                cur_manifest,
                evidence,
                report_path=f"docs/benchmarks/2026-11-{15 + step:02d}-{cohort['id']}-report.md",
                now=now_step,
            )
            self.assertEqual(entry["cycle"], 1)
            self.assertEqual(entry["cohortId"], cohort["id"])

        # After all cohorts complete Cycle 1, ledger MUST roll over to Cycle 2 weekly
        self.assertEqual(cur_manifest["currentCycle"], 2)
        self.assertEqual(cur_manifest["nextCohortIndex"], 0)
        self.assertEqual(cur_manifest["completedJurisdictionsInCycle"], [])

        # 1 day after rollover, Cycle 2 weekly cohort is NOT yet due
        one_day_later = base_now + dt.timedelta(days=total_cohorts)
        status_not_due = compute_status(cur_manifest, now=one_day_later)
        self.assertEqual(status_not_due["cadencePhase"], "steadyState")
        self.assertEqual(status_not_due["frequency"], "weekly")
        self.assertFalse(status_not_due["isDue"])

        # 7 days after rollover, Cycle 2 weekly cohort IS due
        seven_days_later = base_now + dt.timedelta(days=total_cohorts + 6)
        status_due = compute_status(cur_manifest, now=seven_days_later)
        self.assertTrue(status_due["isDue"])

    def test_advance_rejects_failed_or_mismatched_or_premature_evidence(self):
        manifest = copy.deepcopy(load_manifest(DEFAULT_MANIFEST_PATH))
        manifest["currentCycle"] = 1
        manifest["nextCohortIndex"] = 0
        now = dt.datetime(2026, 11, 15, 7, 0, tzinfo=dt.timezone.utc)
        manifest["lastRunAt"] = format_iso_utc(now - dt.timedelta(hours=2))
        manifest["lastAdvancedAt"] = format_iso_utc(now - dt.timedelta(hours=2))

        due_cohort = manifest["cohorts"][0]
        due_id = due_cohort["id"]
        due_codes = [j["code"] for j in due_cohort["jurisdictions"]]

        # 1) Failing evidence rejected
        with self.assertRaisesRegex(ValueError, "did not pass all gates"):
            advance_manifest(
                manifest,
                {
                    "cohort_id": due_id,
                    "overall_status": "FAIL",
                    "all_checks_passed": False,
                    "total_checks": 10,
                    "passed_checks": 9,
                    "failed_checks": 1,
                    "jurisdictions_verified": due_codes,
                },
                now=now,
                force=True,
            )

        # 2) Mismatched cohort rejected
        with self.assertRaisesRegex(ValueError, "does not match due cohort"):
            advance_manifest(
                manifest,
                {
                    "cohort_id": "cohort-03-detroit-miami",
                    "overall_status": "PASS",
                    "all_checks_passed": True,
                    "total_checks": 10,
                    "passed_checks": 10,
                    "failed_checks": 0,
                    "jurisdictions_verified": due_codes,
                },
                now=now,
                force=True,
            )

        # 3) Premature advance without force=True rejected
        with self.assertRaisesRegex(RuntimeError, "not yet due"):
            advance_manifest(
                manifest,
                {
                    "cohort_id": due_id,
                    "overall_status": "PASS",
                    "all_checks_passed": True,
                    "total_checks": 10,
                    "passed_checks": 10,
                    "failed_checks": 0,
                    "jurisdictions_verified": due_codes,
                },
                now=now,
                force=False,
            )


class TestDataPipelineGuards(unittest.TestCase):
    def test_normalize_ct_geodata_columns_bom_duplicates_and_spaces(self):
        raw_df = pd.DataFrame(
            [[1, 99, "10 MAIN ST", "10 MAIN ST DUP", "EAST HARTFORD", 50000, 150000, "ACME LLC"]],
            columns=[
                "\ufeffOBJECTID",
                "OBJECTID",
                "Location",
                "Location",
                "Town_Name",
                "Appraised Land",
                "Appraised Building",
                "Owner",
            ],
        )
        norm_df = _normalize_ct_geodata_columns(raw_df)
        self.assertEqual(list(norm_df.columns).count("OBJECTID"), 1)
        self.assertEqual(list(norm_df.columns).count("Location"), 1)
        self.assertIn("Town Name", norm_df.columns)
        self.assertIn("Appraised_Land", norm_df.columns)
        self.assertIn("Appraised_Building", norm_df.columns)
        self.assertEqual(int(norm_df.iloc[0]["OBJECTID"]), 1)
        self.assertEqual(norm_df.iloc[0]["Location"], "10 MAIN ST")

    def test_parse_source_date_supports_dc_slash_iso_and_epoch_ms(self):
        self.assertEqual(
            parse_source_date("10/1/2025 12:00:00 AM"),
            dt.date(2025, 10, 1),
        )
        self.assertEqual(
            parse_source_date("2026-03-14T15:30:00.000Z"),
            dt.date(2026, 3, 14),
        )
        # 2025-01-15 00:00:00 UTC in ms
        self.assertEqual(
            parse_source_date(1736899200000),
            dt.date(2025, 1, 15),
        )
        self.assertIsNone(parse_source_date(""))
        self.assertIsNone(parse_source_date("not-a-date"))

    def test_ny_dos_commercial_agent_and_clean_str_guards(self):
        self.assertTrue(is_commercial_agent_name("CORPORATION SERVICE COMPANY"))
        self.assertTrue(is_commercial_agent_name("C T CORPORATION SYSTEM"))
        self.assertTrue(is_commercial_agent_name("LEGALINC CORPORATE SERVICES INC."))
        self.assertTrue(is_commercial_agent_name("C/O REGISTERED AGENT"))
        self.assertTrue(is_commercial_agent_name("THE LLC"))
        self.assertFalse(is_commercial_agent_name("JANE DOE"))
        self.assertIsNone(clean_str("  NULL "))
        self.assertIsNone(clean_str("N/A"))
        self.assertEqual(clean_str("  123   BROADWAY  "), "123 BROADWAY")

    def test_city_routes_institutional_filter_never_matches_provenance_metadata(self):
        import os

        city_routes_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "api",
            "city_routes.py",
        )
        with open(city_routes_path, "r", encoding="utf-8") as f:
            src = f.read()

        # Raw connection_signals::text contains "OFFICIAL_MUNICIPAL_PROPERTY_RECORDS",
        # which would match MUNICIPAL in INSTITUTIONAL_NETWORK_PATTERN and hide 100% of private landlords.
        self.assertNotIn("COALESCE(n.connection_signals::text, '')", src)
        self.assertIn("n.connection_signals::jsonb ->> 'primary_entity_name'", src)
        self.assertIn("n.connection_signals::jsonb ->> 'primary_human_name'", src)

    def test_safe_network_refresh_never_deletes_live_summary_cache_and_landing_handles_timeout(self):
        import os

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "api", "safe_network_refresh.py"), "r", encoding="utf-8") as f:
            refresh_src = f.read()
        self.assertNotIn("DELETE FROM kv_cache WHERE key LIKE 'dashboard_summary:%'", refresh_src)
        self.assertIn("dashboard_summary:v2:ct:STATEWIDE", refresh_src)

        with open(os.path.join(root, "frontend", "src", "components", "DatasetLanding.jsx"), "r", encoding="utf-8") as f:
            landing_src = f.read()
        self.assertIn("statsStatusByDataset", landing_src)
        self.assertIn("Public source records ready", landing_src)
        self.assertIn('<progress', landing_src)

    def test_ui_source_only_and_agency_attribution_guards(self):
        import os

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "frontend", "src", "components", "CityExplorer.jsx"), "r", encoding="utf-8") as f:
            explorer_src = f.read()
        with open(os.path.join(root, "frontend", "src", "components", "DatasetLanding.jsx"), "r", encoding="utf-8") as f:
            landing_src = f.read()
        with open(os.path.join(root, "frontend", "src", "App.jsx"), "r", encoding="utf-8") as f:
            app_src = f.read()

        # Non-NYC cities must never inherit hardcoded HPD badges/chips/tooltips unconditionally
        self.assertNotIn("{fmt(n.open_violations)} open HPD records", explorer_src)
        self.assertNotIn("config.name === 'Detroit' ? 'Registered Rental' : 'HPD Registered'", explorer_src)
        self.assertNotIn("title={`${openAll} total open HPD violation records`}", explorer_src)
        for label in (
            "openRecordChipLabel: 'open DHCD notices'",
            "openRecordChipLabel: 'open DOB cases'",
            "openRecordChipLabel: 'open ISD violations'",
            "openRecordChipLabel: 'open blight tickets'",
            "openRecordChipLabel: 'open code violations'",
            "registrationBadge: 'DHCD Registered'",
            "registrationBadge: 'DLCP Licensed Rental'",
            "registrationBadge: 'DCA BHI Registered'",
        ):
            self.assertIn(label, explorer_src)

        # Source-only UI rule: no hardcoded fallback numeric count literals
        for forbidden_literal in ("'3.6M+'", "'1.3M+'", "'768.4K'", "'196K'", "196,000+ HPD registrations"):
            self.assertNotIn(forbidden_literal, landing_src)
            self.assertNotIn(forbidden_literal, app_src)

        with open(os.path.join(root, "api", "main.py"), "r", encoding="utf-8") as f:
            main_src = f.read()
        self.assertNotIn('"unit_count": int(p.get("units_total") or p.get("units_res") or 1)', main_src)
        self.assertNotIn("violations_last_365d AS violations_last_90d", main_src)
        self.assertNotIn("SUM(COALESCE(s.violations_last_365d, s.violations_open, 0))", main_src)


if __name__ == "__main__":
    unittest.main()
