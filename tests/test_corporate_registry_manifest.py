import unittest

from api.corporate_registry_manifest import (
    CORPORATE_REGISTRY_MANIFEST,
    DEFAULT_OC_SNAPSHOT_DATE,
    build_corporate_registry_summary,
    get_registry_policy,
    refresh_status_for_mode,
)
from scripts.ingest_state_corporate_registries import (
    BulkOCStagingWriter,
    NONPROFIT_COMPANY_TYPE_RE,
)


class TestCorporateRegistryManifest(unittest.TestCase):
    def test_all_loaded_registries_classified_honestly(self):
        self.assertEqual(len(CORPORATE_REGISTRY_MANIFEST), 23)
        for jur, entry in CORPORATE_REGISTRY_MANIFEST.items():
            mode = entry["refresh_mode"]
            self.assertIn(mode, ("live_full", "live_partial", "snapshot_frozen"), jur)
            self.assertTrue(entry.get("coverage_note"), f"Missing coverage_note on {jur}")
            if mode == "snapshot_frozen":
                self.assertFalse(entry["entities_live"], jur)
                self.assertFalse(entry["officers_live"], jur)
                self.assertTrue(entry.get("frozen_reason"), f"Frozen registry {jur} must explain why it is frozen")
                self.assertEqual(refresh_status_for_mode(mode), "snapshot_frozen")
            elif mode == "live_full":
                self.assertTrue(entry["entities_live"], jur)
                self.assertTrue(entry["officers_live"], jur)
                self.assertTrue(entry.get("live_registry_name"), jur)
                self.assertEqual(refresh_status_for_mode(mode), "success")
            elif mode == "live_partial":
                self.assertTrue(entry["entities_live"], jur)
                self.assertFalse(entry["officers_live"], jur)
                self.assertTrue(entry.get("frozen_reason"), jur)
                self.assertEqual(refresh_status_for_mode(mode), "partial_live")

    def test_summary_partitions_live_vs_frozen_states(self):
        summary = build_corporate_registry_summary({})
        self.assertEqual(summary["snapshot_date"], DEFAULT_OC_SNAPSHOT_DATE)
        self.assertEqual(summary["live_full_states"], ["CT", "DC", "FL", "PA"])
        self.assertEqual(summary["live_partial_states"], ["CO", "NY"])
        self.assertEqual(
            summary["snapshot_frozen_states"],
            ["AR", "CA", "IL", "MA", "MD", "MI", "MN", "MT", "NC", "NJ", "OH", "VA", "VT", "WI"],
        )
        self.assertEqual(summary["frozen_holding_haven_states"], ["DE", "NV", "WY"])
        self.assertEqual(len(summary["registries"]), 23)
        self.assertIn("2026-11-17", summary["summary_note"])

    def test_unknown_jurisdiction_fails_closed_as_snapshot_frozen(self):
        pol = get_registry_policy("us_zz")
        self.assertEqual(pol["refresh_mode"], "snapshot_frozen")
        self.assertFalse(pol["entities_live"])
        self.assertFalse(pol["officers_live"])

    def test_nonprofit_company_type_detection_and_non_destructive_default(self):
        self.assertIsNotNone(NONPROFIT_COMPANY_TYPE_RE.search("Domestic Non-Profit Corporation"))
        self.assertIsNotNone(NONPROFIT_COMPANY_TYPE_RE.search("Non-Stock"))
        self.assertIsNone(NONPROFIT_COMPANY_TYPE_RE.search("Domestic Limited Liability Company"))
        import inspect
        sig = inspect.signature(BulkOCStagingWriter.__init__)
        self.assertIs(sig.parameters["replace_all"].default, False)


if __name__ == "__main__":
    unittest.main()
