import unittest
from scripts.refresh_federal_and_spot_layers_local import (
    ALL_LAYERS,
    COUNTY_PARCEL_PROBES,
    FEDERAL_SUBSIDY_PROBES,
    MT_STATEWIDE_PROBES,
    ROCHESTER_CODE_PROBES,
    SYRACUSE_PROBES,
    epoch_ms_to_iso_date,
    watermarks_changed,
)


class TestLocalSpotRefreshers(unittest.TestCase):
    def test_epoch_ms_to_iso_date(self):
        self.assertEqual(epoch_ms_to_iso_date(1763596800000), "2025-11-20")
        self.assertIsNone(epoch_ms_to_iso_date(None))
        self.assertIsNone(epoch_ms_to_iso_date(0))
        self.assertIsNone(epoch_ms_to_iso_date("not-a-timestamp"))

    def test_watermarks_unchanged_skips(self):
        prev = {
            "HUD_LIHTC": {"last_edit_ms": 1700000000000, "last_edit_date": "2023-11-14", "count": 50566},
            "HUD_MF_ASSISTED": {"last_edit_ms": 1700000000000, "last_edit_date": "2023-11-14", "count": 23771},
        }
        cur = {
            "HUD_LIHTC": {"ok": True, "last_edit_ms": 1700000000000, "last_edit_date": "2023-11-14", "count": 50566},
            "HUD_MF_ASSISTED": {"ok": True, "last_edit_ms": 1700000000000, "last_edit_date": "2023-11-14", "count": 23771},
        }
        changed, reasons = watermarks_changed(prev, cur)
        self.assertFalse(changed)
        self.assertEqual(reasons, [])

    def test_watermarks_detect_edit_date_and_count_deltas(self):
        prev = {
            "HUD_MF_ASSISTED": {"last_edit_ms": 1700000000000, "last_edit_date": "2023-11-14", "count": 23771},
            "USDA_RURAL_515": {"last_edit_ms": None, "last_edit_date": None, "count": 13262},
        }
        cur = {
            "HUD_MF_ASSISTED": {"ok": True, "last_edit_ms": 1705000000000, "last_edit_date": "2024-01-11", "count": 23771},
            "USDA_RURAL_515": {"ok": True, "last_edit_ms": None, "last_edit_date": None, "count": 13300},
        }
        changed, reasons = watermarks_changed(prev, cur)
        self.assertTrue(changed)
        self.assertEqual(len(reasons), 2)
        self.assertTrue(any("HUD_MF_ASSISTED:edit_date" in r for r in reasons))
        self.assertTrue(any("USDA_RURAL_515:count" in r for r in reasons))

    def test_watermarks_fail_closed_when_upstream_unreachable(self):
        prev = {"HUD_LIHTC": {"last_edit_ms": 1700000000000, "count": 50566}}
        cur = {"HUD_LIHTC": {"ok": False, "last_edit_ms": None, "count": None, "error": "Timeout"}}
        changed, reasons = watermarks_changed(prev, cur)
        self.assertFalse(changed)
        self.assertEqual(reasons, ["all_upstream_probes_unreachable"])

    def test_probe_registries_cover_expected_layers(self):
        self.assertEqual(set(FEDERAL_SUBSIDY_PROBES.keys()), {"HUD_LIHTC", "HUD_MF_ASSISTED", "HUD_INSURED_MF", "USDA_RURAL_515"})
        self.assertEqual(set(SYRACUSE_PROBES.keys()), {"SYRACUSE_CODE_VIOLATIONS", "SYRACUSE_RENTAL_REGISTRY"})
        self.assertEqual(set(ROCHESTER_CODE_PROBES.keys()), {"ROCHESTER_NBD_CODE"})
        self.assertEqual(set(MT_STATEWIDE_PROBES.keys()), {"MT_MSDI_ORION"})
        self.assertEqual(
            set(COUNTY_PARCEL_PROBES.keys()),
            {"oh_cuyahoga", "oh_v3", "oh_v4", "oh_summit", "mn_metro", "mn_v3", "mn_v4", "il_lake", "il_v4", "ny_rochester"},
        )
        self.assertEqual(
            set(ALL_LAYERS),
            {"federal_subsidies", "syracuse_ny", "rochester_ny", "city_licensing_and_enrichment", "county_parcels", "mt_statewide", "geocode_backfill"},
        )


if __name__ == "__main__":
    unittest.main()
