"""
Unit tests for deterministic, source-backed network operator profiling (`api/network_profile.py`)
and post-rebuild representative-label auditing (`updater/post_rebuild_label_audit.py`).
"""

import unittest

from api.network_profile import (
    clean_display_hq,
    derive_network_business_profile,
    extract_care_of_orgs,
    hq_street_zip_key,
)
from updater.post_rebuild_label_audit import (
    _needs_external_web_verification,
    build_label_audit_email_html,
)
from api.audit_review_routes import verify_review_token
from urllib.parse import parse_qs, urlparse


class TestNetworkProfile(unittest.TestCase):
    def test_winncompanies_selected_from_care_of_and_winn_stem_not_spv_or_jv_partner(self):
        members = [
            "27 JACKSON LIMITED PARTNERSHIP",
            "CANAL LOFTS WINN LLC",
            "COUNTING HOUSE WINN LLC",
            "EAST MILLS WINN LIMITED PARTNERSHIP",
            "LUDLOW MILL HOUSING WINN LLC",
            "NORTHERN HEIGHTS WINN LIMITED PARTNERSHIP",
            "RIVERSIDE AVENUE WINN LIMITED PARTNERSHIP",
            "WINN BROOKLINE VILLAGE LLC",
            "WINN LLC MANAGER, INC.",
            "WINN MANAGER CORP.",
        ]
        oc_enr = {
            "corp_parents": [
                ("URBAN EDGE HOUSING CORPORATION", 3),
                ("WINN LLC MANAGER, INC", 1),
            ],
            "oc_care_of": [
                ("HEBREW SENIORLIFE", 5),
                ("URBAN EDGE HOUSING CORPORATION", 4),
                ("WINNCOMPANIES", 4),
            ],
            "oc_hq_addresses": [
                ("ONE WASHINGTON MALL, SUITE 500, BOSTON, MA, 02108", 24),
                ("4 GERRISH AVE., CHELSEA, MA, 02150", 4),
            ],
            "direct_officer_counts": [
                ("ALPHONSO PETERS", 3),
            ],
            "hq_operating_cos": [
                {
                    "name": "WINN DEVELOPMENT CORP.",
                    "shared_officers": 3,
                    "officers": ["GILBERT J WINN", "LAWRENCE H CURTIS", "MICHAEL T PUTZIGER"],
                    "shares_hq": True,
                    "is_primary_hq": True,
                    "care_of_org": "WINNCOMPANIES",
                },
                {
                    "name": "WINNCOMPANIES CHARITABLE SCHOLARSHIP FOUNDATION, INC.",
                    "shared_officers": 2,
                    "officers": ["GILBERT J WINN", "MICHAEL T PUTZIGER"],
                    "shares_hq": True,
                    "is_primary_hq": True,
                    "care_of_org": None,
                },
            ],
        }
        prof = derive_network_business_profile(
            "27 JACKSON LIMITED PARTNERSHIP",
            members,
            ["C/O USA, 1 WASHINGTON MALL STE 500, BOSTON, MA, 02108"],
            {
                "primary_human_name": "AN C DUONG",
                "source_human_names": [
                    "AN C DUONG",
                    "GILBERT J. WINN",
                    "LAWRENCE H. CURTIS",
                    "MICHAEL T. PUTZIGER",
                ],
            },
            oc_enr,
        )
        self.assertEqual(prof["operator_name"], "WINNCOMPANIES")
        self.assertEqual(prof["operator_source"], "corporate_care_of")
        self.assertIn("GILBERT", (prof["representative_human_name"] or "").upper())
        self.assertIn("ONE WASHINGTON MALL", (prof["primary_hq_address"] or "").upper())
        self.assertNotIn(
            "WINNCOMPANIES CHARITABLE SCHOLARSHIP FOUNDATION, INC.",
            prof["operating_entities"],
        )

    def test_hamilton_company_beats_homer_apartments_spv(self):
        members = [
            "HOMER APARTMENTS LIMITED PARTNERSHIP",
            "HAMILTON ASSOCIATES LLC",
            "HAMILTON BATTLE GREEN LLC",
            "HAMILTON BELMONT LLC",
            "HAMILTON REALTY LP",
        ]
        oc_enr = {
            "corp_parents": [("THE HAMILTON COMPANY, INC.", 32), ("NEWREAL, INC", 5)],
            "oc_care_of": [("THE HAMILTON COMPANY", 4)],
            "oc_hq_addresses": [("39 BRIGHTON AVE., ALLSTON, MA, 02134-0000", 57)],
            "direct_officer_counts": [("MARK LEPLER", 29), ("ALEX PESELMAN", 11)],
            "hq_operating_cos": [],
        }
        prof = derive_network_business_profile(
            "HOMER APARTMENTS LIMITED PARTNERSHIP",
            members,
            ["39 BRIGHTON AVE, ALLSTON, MA, 02134"],
            {"primary_human_name": "JAMESON BROWN", "source_human_names": ["JAMESON BROWN", "MARK LEPLER"]},
            oc_enr,
        )
        self.assertIn("HAMILTON COMPANY", (prof["operator_name"] or "").upper())
        self.assertEqual(prof["operator_source"], "corporate_parent")
        self.assertEqual(prof["primary_hq_address"], "39 BRIGHTON AVE., ALLSTON, MA, 02134")

    def test_samia_companies_beats_street_spv_with_ljs_initials(self):
        members = [
            "740 CENTRAL LJS LLC",
            "GARDNER ST REALTY LJS LLC",
            "170 KELTON LJS LLC",
            "19 BRAEMORE LJS LLC",
            "217 ESSEX LJS LLC",
            "SAMIA ASSOCIATES I LLC",
            "SAMIA PROPERTIES I LJS LLC",
            "SAMIA PROPERTIES II LJS LLC",
        ]
        oc_enr = {
            "corp_parents": [],
            "oc_care_of": [],
            "oc_hq_addresses": [("60 LEO BIRMINGHAM PARKWAY - 3RD FLOOR, BRIGHTON, MA, 02135", 22)],
            "direct_officer_counts": [("LEONARD J. SAMIA", 48)],
            "hq_operating_cos": [
                {
                    "name": "SAMIA COMPANIES LLC",
                    "shared_officers": 2,
                    "officers": ["LEONARD J SAMIA", "ALEXANDRA SAMIA SOLIA"],
                    "shares_hq": True,
                    "is_primary_hq": True,
                    "care_of_org": None,
                }
            ],
        }
        prof = derive_network_business_profile(
            "740 CENTRAL LJS LLC",
            members,
            ["C/O USA, 60 LEO BIRMINGHAM PKWY, BRIGHTON, MA, 02135"],
            {"primary_human_name": "LEONARD J SAMIA", "source_human_names": ["LEONARD J SAMIA"]},
            oc_enr,
        )
        self.assertEqual(prof["operator_name"], "SAMIA COMPANIES LLC")
        self.assertEqual(prof["representative_human_name"], "LEONARD J. SAMIA")

    def test_never_fabricates_operator_for_individual_homeowner_or_person_spvs(self):
        prof_homeowner = derive_network_business_profile(
            "JORDAN GIBBS",
            ["JORDAN GIBBS"],
            [],
            {"primary_human_name": "JORDAN GIBBS", "source_human_names": ["JORDAN GIBBS"]},
            {},
        )
        self.assertIsNone(prof_homeowner["operator_name"])
        self.assertEqual(prof_homeowner["representative_human_name"], "JORDAN GIBBS")

        prof_nyc_spvs = derive_network_business_profile(
            "BROWN GLEN",
            ["109 MONTAGUE LLC", "11 GOODWIN PL LLC", "1146 ST MARKS AVE LLC"],
            ["625 BROADWAY 11 10012"],
            {},
            {},
        )
        self.assertIsNone(prof_nyc_spvs["operator_name"])
        self.assertEqual(prof_nyc_spvs["business_count"], 3)
        self.assertEqual(prof_nyc_spvs["primary_hq_address"], "625 BROADWAY 11 10012")

    def test_hq_normalization_and_officer_deduplication(self):
        self.assertEqual(
            hq_street_zip_key("ONE WASHINGTON MALL, SUITE 500, BOSTON, MA, 02108"),
            hq_street_zip_key("C/O USA, 1 WASHINGTON MALL STE 500, BOSTON, MA, 02108"),
        )
        self.assertIsNone(hq_street_zip_key("New Castle, DE"))
        self.assertEqual(
            clean_display_hq("C/O INVITATION HOMES TAX DEPT 1717 MAIN ST STE 2000 DALLAS TX 75201"),
            "1717 MAIN ST STE 2000 DALLAS TX 75201",
        )
        co = extract_care_of_orgs(
            [
                "ONE WASHINGTON MALL, SUITE 500, C/O WINNCOMPANIES, BOSTON, MA, 02108",
                "C/O USA, 126 JOHN ST, LOWELL, MA, 01852",
            ]
        )
        self.assertEqual(co.get("WINNCOMPANIES"), 1)
        self.assertNotIn("USA", co)

        prof = derive_network_business_profile(
            "OLD COLONY REALTY PARTNERS LLC",
            ["OLD COLONY REALTY PARTNERS LLC"],
            [],
            {
                "primary_human_name": "SEAN E. WILDER",
                "source_human_names": [
                    "SEAN E. WILDER",
                    "BENJAMIN J WILDER",
                    "WILDER BENJAMIN",
                    "David Veneziano David Veneziano",
                    "TRUSTEE ALEX PESELMAN",
                ],
            },
            {},
        )
        upper_officers = [o.upper() for o in prof["key_officers"]]
        # Reversed duplicate collapsed into one Wilder entry
        wilder_ben = [o for o in upper_officers if "BENJAMIN" in o]
        self.assertEqual(len(wilder_ben), 1)
        # Exact duplicated full name collapsed
        self.assertTrue(all("VENEZIANO DAVID VENEZIANO" not in o for o in upper_officers))
        # Leading TRUSTEE stripped
        self.assertTrue(all(not o.startswith("TRUSTEE ") for o in upper_officers))

    def test_web_audit_trigger_logic_flags_spvs_but_skips_verified_operators(self):
        self.assertTrue(
            _needs_external_web_verification(
                {"building_count": 497},
                {"operator_name": "OBSIDIAN ML 6 LLC", "operator_source": "deed_owner"},
            )
        )
        self.assertFalse(
            _needs_external_web_verification(
                {"building_count": 908},
                {"operator_name": "WINNCOMPANIES", "operator_source": "corporate_care_of"},
            )
        )
        self.assertFalse(
            _needs_external_web_verification(
                {"building_count": 10},
                {"operator_name": "123 MAIN ST LLC", "operator_source": "deed_owner"},
            )
        )

    def test_wc_smith_beats_skyland_block_2_project_spv(self):
        prof = derive_network_business_profile(
            "SKYLAND PHASE C-2, LLC",
            [
                "SKYLAND BLOCK 2 RESIDENTIAL, LLC",
                "WILLIAM C. SMITH & CO., INC.",
                "PENN PROPERTIES LLC",
            ],
            ["1100 NEW JERSEY AVENUE, SE, SUITE 1000, WASHINGTON, DC, 20003"],
            {
                "primary_human_name": "BRAD FENNELL",
                "source_human_names": ["WILLIAM CHRISTOPHER SMITH", "BRAD FENNELL"],
            },
            {
                "oc_care_of": [("WILLIAM C. SMITH & CO., INC.", 2)],
                "oc_hq_addresses": [("1100 NEW JERSEY AVENUE, SE, SUITE 1000, WASHINGTON, DC, 20003", 12)],
                "hq_operating_cos": [
                    {
                        "name": "SKYLAND BLOCK 2 RESIDENTIAL, LLC",
                        "shared_officers": 2,
                        "officers": ["WILLIAM CHRISTOPHER SMITH", "SARAH CONSTANT"],
                        "shares_hq": True,
                        "is_primary_hq": True,
                        "care_of_org": None,
                    },
                    {
                        "name": "WILLIAM C. SMITH & CO., INC.",
                        "shared_officers": 2,
                        "officers": ["WILLIAM CHRISTOPHER SMITH", "SARAH CONSTANT"],
                        "shares_hq": True,
                        "is_primary_hq": True,
                        "care_of_org": None,
                    },
                ],
            },
        )
        self.assertEqual(prof["operator_name"], "WILLIAM C. SMITH & CO., INC.")

    def test_label_audit_email_generates_valid_hmac_signed_approve_and_decline_buttons(self):
        cands = [
            {
                "id": 101,
                "jurisdiction": "ma",
                "network_key": "network_ma_test_1",
                "current_display_name": "27 JACKSON LIMITED PARTNERSHIP",
                "current_operator_name": "27 JACKSON LIMITED PARTNERSHIP",
                "suggested_operator_name": "WinnCompanies",
                "suggested_representative_person": "Gilbert J. Winn",
                "suggested_hq_address": "One Washington Mall, Suite 500, Boston, MA 02108",
                "rationale": "WinnCompanies portfolio page lists Jackson Commons & multiple MA properties in this network.",
                "evidence_urls": ["https://www.winncompanies.com/properties"],
                "evidence_snippets": ["Owned and managed by WinnDevelopment / WinnResidential"],
                "building_count": 908,
                "entity_count": 234,
            },
            {
                "id": 102,
                "jurisdiction": "dc",
                "network_key": "network_dc_test_2",
                "current_display_name": "SKYLAND PHASE C-2, LLC",
                "current_operator_name": "SKYLAND BLOCK 2 RESIDENTIAL, LLC",
                "suggested_operator_name": "WC Smith",
                "suggested_representative_person": "William Christopher Smith",
                "suggested_hq_address": "1100 New Jersey Ave SE Suite 1000, Washington, DC 20003",
                "rationale": "WC Smith website lists Skyland Town Center and Parklands properties at 1100 New Jersey Ave SE.",
                "evidence_urls": ["https://www.wcsmith.com/"],
                "evidence_snippets": ["WC Smith — 1100 New Jersey Ave SE"],
                "building_count": 239,
                "entity_count": 47,
            },
        ]
        subject, html_body, text_body = build_label_audit_email_html(
            cands,
            [{"jurisdiction": "ma", "updated": 76}, {"jurisdiction": "dc", "updated": 60}],
            base_url="https://theyownwhat.net",
            reviewer="salmun@storrsmagic.org",
        )
        self.assertIn("2 Representative Network Labels", subject)
        self.assertIn("Approve &amp; Apply Label", html_body)
        self.assertIn("Approve All 2 Pending Labels", html_body)
        self.assertIn("https://www.winncompanies.com/properties", html_body)
        self.assertIn("label-candidate%3A101", text_body)

        # Extract the signed Approve URL from plain-text body and verify its HMAC token
        approve_line = next(ln for ln in text_body.splitlines() if "Approve:" in ln and "label-candidate%3A101" in ln)
        approve_url = approve_line.split("Approve:", 1)[1].strip()
        qs = parse_qs(urlparse(approve_url).query)
        self.assertEqual(qs["decision"][0], "approve")
        self.assertEqual(qs["item_key"][0], "label-candidate:101")
        ok, payload, err = verify_review_token(qs["token"][0], expected_item_key="label-candidate:101")
        self.assertTrue(ok, err)
        self.assertEqual(payload["r"], "salmun@storrsmagic.org")


if __name__ == "__main__":
    unittest.main()
