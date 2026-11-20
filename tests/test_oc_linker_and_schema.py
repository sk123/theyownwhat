import datetime as dt
import unittest

from api.nationwide_schema import validate_jurisdiction_slug
from api.oc_linker import (
    OCCandidate,
    classify_oc_officer_role,
    demangle_assessor_owner,
    is_generic_homonym_stem,
    score_oc_candidates,
)
from api.shared_utils import normalize_business_name, normalize_mailing_address_coarse


class TestOCLinkerAndAssessorDemangler(unittest.TestCase):
    def test_validate_jurisdiction_slug(self):
        self.assertEqual(validate_jurisdiction_slug("MA"), "ma")
        self.assertEqual(validate_jurisdiction_slug("fl_miami"), "fl_miami")
        with self.assertRaises(ValueError):
            validate_jurisdiction_slug("ma; DROP TABLE")

    def test_demangle_assessor_owner_strips_tenancy_and_extracts_care_of(self):
        rec = demangle_assessor_owner(
            "BEACON HILL REALTY HOLDINGS LLC ET AL",
            owner2="C/O ACME MANAGEMENT GROUP",
            mailing_address="125 SUMMER ST STE 400, BOSTON, MA 02110",
        )
        self.assertTrue(rec.is_business)
        self.assertEqual(rec.cleaned_owner, "BEACON HILL REALTY HOLDINGS LLC")
        self.assertEqual(rec.care_of_entity, "ACME MANAGEMENT GROUP")
        self.assertEqual(rec.mailing_state, "MA")
        self.assertFalse(rec.is_truncated)

    def test_demangle_detects_fixed_width_truncation(self):
        raw_30 = "COMMONWEALTH MULTIFAMILY PROPE"  # 30 chars truncated mid-word
        rec = demangle_assessor_owner(
            raw_30,
            mailing_address="50 MILK ST, BOSTON, MA 02109",
            col_max_len=30,
        )
        self.assertTrue(rec.is_truncated)
        self.assertIsNotNone(rec.prefix18)

    def test_officer_role_classifier_blocks_agents_and_detects_corp_parents(self):
        self.assertEqual(
            classify_oc_officer_role("Registered Agent", "CT CORPORATION SYSTEM"),
            ("agent_or_incorporator", False),
        )
        self.assertEqual(
            classify_oc_officer_role("Incorporator", "JANE ATTORNEY ESQ"),
            ("agent_or_incorporator", False),
        )
        self.assertEqual(
            classify_oc_officer_role("Managing Member", "GREYSTARS PARENT HOLDINGS LLC"),
            ("corporate_parent", True),
        )
        self.assertEqual(
            classify_oc_officer_role("Manager", "MENACHEM GUREVITCH"),
            ("beneficial_principal", False),
        )

    def test_anti_homonym_quarantines_uncorroborated_out_of_state_and_duplicate_locals(self):
        demangled = demangle_assessor_owner(
            "SUNRISE REALTY LLC",
            mailing_address="99 UNKNOWN PO BOX, HARTFORD, CT 06103",
        )
        parcel_coarse = normalize_mailing_address_coarse("99 UNKNOWN RD, HARTFORD, CT 06103")

        # 1) Out-of-state FL + DE homonyms with NO local/mail-state match & NO address match -> None
        fl_cand = OCCandidate(
            jurisdiction_code="us_fl",
            company_number="L100",
            name="SUNRISE REALTY LLC",
            name_norm=normalize_business_name("SUNRISE REALTY LLC"),
            mailing_address_coarse=normalize_mailing_address_coarse("500 BISCAYNE BLVD, MIAMI, FL 33132"),
        )
        de_cand = OCCandidate(
            jurisdiction_code="us_de",
            company_number="D200",
            name="SUNRISE REALTY LLC",
            name_norm=normalize_business_name("SUNRISE REALTY LLC"),
            registered_address_coarse=normalize_mailing_address_coarse("1209 ORANGE ST, WILMINGTON, DE 19801"),
            agent_address_coarse=normalize_mailing_address_coarse("1209 ORANGE ST, WILMINGTON, DE 19801"),
        )
        self.assertIsNone(
            score_oc_candidates(demangled, "CT", parcel_coarse, [fl_cand, de_cand])
        )

        # 2) Two distinct active CT companies sharing the exact same name without address match -> Quarantine (None)
        ct1 = OCCandidate(
            jurisdiction_code="us_ct",
            company_number="CT1",
            name="SUNRISE REALTY LLC",
            name_norm=normalize_business_name("SUNRISE REALTY LLC"),
            mailing_address_coarse=normalize_mailing_address_coarse("10 MAIN ST, STAMFORD, CT 06901"),
        )
        ct2 = OCCandidate(
            jurisdiction_code="us_ct",
            company_number="CT2",
            name="SUNRISE REALTY LLC",
            name_norm=normalize_business_name("SUNRISE REALTY LLC"),
            mailing_address_coarse=normalize_mailing_address_coarse("20 ELM ST, NEW HAVEN, CT 06510"),
        )
        self.assertIsNone(
            score_oc_candidates(demangled, "CT", parcel_coarse, [ct1, ct2])
        )

    def test_tier1_address_tiebreak_and_branch_to_home_canonicalization(self):
        addr = "399 WHALLEY AVE SUITE 103, NEW HAVEN, CT 06511"
        coarse = normalize_mailing_address_coarse(addr)
        demangled = demangle_assessor_owner("OCEANVIEW APARTMENTS LLC", mailing_address=addr)

        ma_branch = OCCandidate(
            jurisdiction_code="us_ma",
            company_number="MA-FOREIGN-99",
            name="OCEANVIEW APARTMENTS LLC",
            name_norm=normalize_business_name("OCEANVIEW APARTMENTS LLC"),
            mailing_address_coarse=coarse,
            home_jurisdiction_code="us_de",
            home_company_number="DE-HOME-777",
        )
        ma_decoy = OCCandidate(
            jurisdiction_code="us_ma",
            company_number="MA-OTHER-11",
            name="OCEANVIEW APARTMENTS LLC",
            name_norm=normalize_business_name("OCEANVIEW APARTMENTS LLC"),
            mailing_address_coarse=normalize_mailing_address_coarse("1 BEACON ST, BOSTON, MA 02108"),
        )
        res = score_oc_candidates(demangled, "MA", coarse, [ma_branch, ma_decoy])
        self.assertIsNotNone(res)
        self.assertEqual(res.match_tier, 1)
        self.assertAlmostEqual(res.confidence, 0.99)
        self.assertEqual(res.canonical_jurisdiction_code, "us_de")
        self.assertEqual(res.canonical_company_number, "DE-HOME-777")

    def test_registered_agent_address_never_corroborates_tier1_or_promotes_mail_state(self):
        ra_raw = "1209 ORANGE ST, WILMINGTON, DE 19801"
        ra_coarse = normalize_mailing_address_coarse(ra_raw)
        demangled = demangle_assessor_owner("BLUEWATER ASSET VENTURES LLC", mailing_address=ra_raw)

        de_cand = OCCandidate(
            jurisdiction_code="us_de",
            company_number="DE-999",
            name="BLUEWATER ASSET VENTURES LLC",
            name_norm=normalize_business_name("BLUEWATER ASSET VENTURES LLC"),
            registered_address_coarse=ra_coarse,
            mailing_address_coarse=ra_coarse,
            agent_address_coarse=ra_coarse,
        )
        # Even though mailing_state == 'DE', 1209 ORANGE ST is a commercial RA hub, so
        # it must neither corroborate Tier 1 nor promote 'us_de' to an operational Tier-2 state!
        self.assertIsNone(
            score_oc_candidates(
                demangled,
                "PA",
                ra_coarse,
                [de_cand],
                registered_agent_addrs={ra_coarse},
            )
        )
        self.assertTrue(is_generic_homonym_stem(normalize_business_name("PROPERTIES LLC")))

    def test_tier3_truncated_prefix_and_tier4_previous_name(self):
        hq = "75 STATE ST, BOSTON, MA 02109"
        hq_coarse = normalize_mailing_address_coarse(hq)

        # Tier 3: 30-char truncated CAMA owner + exact HQ address
        trunc_rec = demangle_assessor_owner(
            "MASSACHUSETTS HARBORFRONT REDE",
            mailing_address=hq,
            col_max_len=30,
        )
        self.assertTrue(trunc_rec.is_business)
        self.assertTrue(trunc_rec.is_truncated)
        full_cand = OCCandidate(
            jurisdiction_code="us_ma",
            company_number="MA-300",
            name="MASSACHUSETTS HARBORFRONT REDEVELOPMENT LLC",
            name_norm=normalize_business_name("MASSACHUSETTS HARBORFRONT REDEVELOPMENT LLC"),
            mailing_address_coarse=hq_coarse,
        )
        res_t3 = score_oc_candidates(trunc_rec, "MA", hq_coarse, [full_cand])
        self.assertIsNotNone(res_t3)
        self.assertEqual(res_t3.match_tier, 3)

        # Tier 4: Previous company name aligned with deed sale date
        prev_rec = demangle_assessor_owner(
            "OLD COLONY APARTMENT PARTNERS LLC",
            mailing_address="100 FEDERAL ST, BOSTON, MA 02110",
        )
        renamed_cand = OCCandidate(
            jurisdiction_code="us_ma",
            company_number="MA-400",
            name="NEW BAY STATE LIVING LLC",
            name_norm=normalize_business_name("NEW BAY STATE LIVING LLC"),
            matched_previous_name_norm=prev_rec.owner_norm,
            previous_name_end_date=dt.date(2024, 6, 1),
        )
        res_t4 = score_oc_candidates(
            prev_rec,
            "MA",
            normalize_mailing_address_coarse("999 OTHER ST, BOSTON, MA 02110"),
            [renamed_cand],
            parcel_last_sale_date=dt.date(2022, 5, 15),
        )
        self.assertIsNotNone(res_t4)
        self.assertEqual(res_t4.match_tier, 4)


class TestOCMultiHopResolutionIntegration(unittest.TestCase):
    def test_postgres_multihop_branch_and_corporate_officer_resolution(self):
        import os
        import psycopg2
        from api.oc_linker import resolve_oc_company_principals

        db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
        try:
            conn = psycopg2.connect(db_url, connect_timeout=3)
        except Exception:
            conn = psycopg2.connect("postgresql://user:password@ctdata_db:5432/ctdata", connect_timeout=3)

        try:
            with conn.cursor() as cur:
                # Seed synthetic multi-hop chain inside an uncommitted transaction:
                #   us_ma/TEST-BRANCH-1 --(home_company)--> us_de/TEST-DE-CHILD-1
                #   us_de/TEST-DE-CHILD-1 --(corporate_parent officer)--> us_de/TEST-DE-HOLDCO-1
                #   us_de/TEST-DE-HOLDCO-1 --(beneficial_principal)--> JANE DOE LANDLORD
                #   plus a registered agent that MUST NOT surface as a beneficial human!
                cur.execute(
                    """
                    INSERT INTO oc_companies (
                        jurisdiction_code, company_number, name, name_norm, is_active,
                        home_jurisdiction_code, home_company_number
                    ) VALUES
                        ('us_ma', 'TEST-BRANCH-1', '100 MAIN WORCESTER LLC', '100 MAIN WORCESTER LLC', TRUE, 'us_de', 'TEST-DE-CHILD-1'),
                        ('us_de', 'TEST-DE-CHILD-1', '100 MAIN WORCESTER LLC', '100 MAIN WORCESTER LLC', TRUE, NULL, NULL),
                        ('us_de', 'TEST-DE-HOLDCO-1', 'BAYSTATE MASTER HOLDCO LLC', 'BAYSTATE MASTER HOLDCO LLC', TRUE, NULL, NULL)
                    """
                )
                cur.execute(
                    """
                    INSERT INTO oc_officers (
                        jurisdiction_code, company_number, name, name_norm,
                        position, role_class, is_corporate_officer, is_current
                    ) VALUES
                        ('us_de', 'TEST-DE-CHILD-1', 'BAYSTATE MASTER HOLDCO LLC', 'BAYSTATE MASTER HOLDCO LLC',
                         'Managing Member', 'corporate_parent', TRUE, TRUE),
                        ('us_de', 'TEST-DE-CHILD-1', 'CORPORATION SERVICE COMPANY', 'CORPORATION SERVICE COMPANY',
                         'Registered Agent', 'agent_or_incorporator', TRUE, TRUE),
                        ('us_de', 'TEST-DE-HOLDCO-1', 'JANE DOE', 'JANE DOE',
                         'Managing Member', 'beneficial_principal', FALSE, TRUE)
                    """
                )
            resolved = resolve_oc_company_principals(conn, [("us_ma", "TEST-BRANCH-1")], max_hops=3)
            info = resolved.get(("us_ma", "TEST-BRANCH-1"), {})
            self.assertEqual(info.get("humans"), ["JANE DOE"])
            self.assertIn("BAYSTATE MASTER HOLDCO LLC", info.get("holding_entities", []))
        finally:
            conn.rollback()
            conn.close()

    def test_bidirectional_branch_unnumbered_home_and_officer_name_cleanup(self):
        import os
        import psycopg2
        from api.oc_linker import (
            clean_oc_officer_name,
            classify_oc_officer_role,
            resolve_oc_company_principals,
        )

        self.assertEqual(clean_oc_officer_name("DAVID TADESSE AUTHORIZED PERSON"), "DAVID TADESSE")
        self.assertEqual(clean_oc_officer_name("JANE DOE, MANAGING MEMBER"), "JANE DOE")
        self.assertEqual(clean_oc_officer_name("GENERAL PARTNER HOLDINGS LLC"), "GENERAL PARTNER HOLDINGS LLC")
        self.assertEqual(
            classify_oc_officer_role("beneficial owner", "HINES INTERESTS", explicit_type="Company"),
            ("corporate_parent", True),
        )
        self.assertEqual(
            classify_oc_officer_role("other officer w/policy-making authority", "SARAH SMITH"),
            ("beneficial_principal", False),
        )

        db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
        try:
            conn = psycopg2.connect(db_url, connect_timeout=3)
        except Exception:
            conn = psycopg2.connect("postgresql://user:password@ctdata_db:5432/ctdata", connect_timeout=3)

        try:
            with conn.cursor() as cur:
                # 1) Anonymous DE parent `TEST-DE-ANON` pierced via `oc_branches` to DC branch `TEST-DC-BR`
                # 2) Unnumbered foreign branch `TEST-VT-UNNUM` (`home_company_number=NULL`, `home_jurisdiction_code='us_de'`)
                #    uniquely matching `TEST-DE-UNIQUE` -> `MARIA LANDLORD`
                cur.execute(
                    """
                    INSERT INTO oc_companies (
                        jurisdiction_code, company_number, name, name_norm, is_active,
                        home_jurisdiction_code, home_company_number
                    ) VALUES
                        ('us_de', 'TEST-DE-ANON', 'ANON CAPITAL PROPCO LLC', 'ANON CAPITAL PROPCO LLC', TRUE, NULL, NULL),
                        ('us_dc', 'TEST-DC-BR', 'ANON CAPITAL PROPCO LLC', 'ANON CAPITAL PROPCO LLC', TRUE, 'us_de', 'TEST-DE-ANON'),
                        ('us_vt', 'TEST-VT-UNNUM', 'GREEN MTN MULTIFAMILY LLC', 'GREEN MTN MULTIFAMILY LLC', TRUE, 'us_de', NULL),
                        ('us_de', 'TEST-DE-UNIQUE', 'GREEN MTN MULTIFAMILY LLC', 'GREEN MTN MULTIFAMILY LLC', TRUE, NULL, NULL)
                    """
                )
                cur.execute(
                    """
                    INSERT INTO oc_branches (
                        branch_jurisdiction_code, branch_company_number,
                        home_jurisdiction_code, home_company_number
                    ) VALUES ('us_dc', 'TEST-DC-BR', 'us_de', 'TEST-DE-ANON')
                    """
                )
                cur.execute(
                    """
                    INSERT INTO oc_officers (
                        jurisdiction_code, company_number, name, name_norm,
                        position, role_class, is_corporate_officer, is_current
                    ) VALUES
                        ('us_dc', 'TEST-DC-BR', 'DAVID TADESSE AUTHORIZED PERSON', 'DAVID TADESSE',
                         'beneficial owner', 'beneficial_principal', FALSE, TRUE),
                        ('us_de', 'TEST-DE-UNIQUE', 'MARIA LANDLORD', 'MARIA LANDLORD',
                         'Manager', 'beneficial_principal', FALSE, TRUE)
                    """
                )
            resolved = resolve_oc_company_principals(
                conn,
                [("us_de", "TEST-DE-ANON"), ("us_vt", "TEST-VT-UNNUM")],
                max_hops=3,
            )
            self.assertEqual(resolved[("us_de", "TEST-DE-ANON")]["humans"], ["DAVID TADESSE"])
            self.assertEqual(resolved[("us_vt", "TEST-VT-UNNUM")]["humans"], ["MARIA LANDLORD"])
        finally:
            conn.rollback()
            conn.close()

    def test_nonprofit_volunteer_board_and_foreign_branch_registered_office_isolation(self):
        import os
        import psycopg2
        from api.oc_linker import resolve_oc_company_principals

        db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
        try:
            conn = psycopg2.connect(db_url, connect_timeout=3)
        except Exception:
            conn = psycopg2.connect("postgresql://user:password@ctdata_db:5432/ctdata", connect_timeout=3)

        try:
            with conn.cursor() as cur:
                # 1) Nonprofit with >2 volunteer trustees must NOT export board humans to bridge private corps,
                #    while preserving its own domestic HQ address.
                # 2) Out-of-state foreign branch (`us_pa`) must NOT export its PA statutory `registered_address_raw`
                #    as a corporate HQ address, while preserving its `mailing_address_raw` HQ.
                cur.execute(
                    """
                    INSERT INTO oc_companies (
                        jurisdiction_code, company_number, name, name_norm, is_active, is_nonprofit,
                        registered_address_raw, mailing_address_raw,
                        home_jurisdiction_code, home_company_number
                    ) VALUES
                        ('us_ma', 'TEST-NP-BOARD', 'ROXBURY HOPE CHARITY INC', 'ROXBURY HOPE CHARITY INCORPORATED',
                         TRUE, TRUE, '550 DUDLEY ST, ROXBURY, MA, 02119', '550 DUDLEY ST, ROXBURY, MA, 02119', NULL, NULL),
                        ('us_ma', 'TEST-CORP-HOME', 'ACME MEDTECH CORP', 'ACME MEDTECH CORPORATION',
                         TRUE, FALSE, '26999 SYNTHETIC MILL RD, CHELMSFORD, MA, 01824', '26999 SYNTHETIC MILL RD, CHELMSFORD, MA, 01824', NULL, NULL),
                        ('us_pa', 'TEST-PA-FOREIGN', 'ACME MEDTECH CORP', 'ACME MEDTECH CORPORATION',
                         TRUE, FALSE, '5235 N FRONT ST, HARRISBURG, PA, 17110', '26999 SYNTHETIC MILL RD, CHELMSFORD, MA, 01824', 'us_ma', 'TEST-CORP-HOME')
                    """
                )
                cur.execute(
                    """
                    INSERT INTO oc_officers (
                        jurisdiction_code, company_number, name, name_norm,
                        position, role_class, is_corporate_officer, is_current
                    ) VALUES
                        ('us_ma', 'TEST-NP-BOARD', 'ALICE TRUSTEE', 'ALICE TRUSTEE', 'Director', 'beneficial_principal', FALSE, TRUE),
                        ('us_ma', 'TEST-NP-BOARD', 'BOB TRUSTEE', 'BOB TRUSTEE', 'Director', 'beneficial_principal', FALSE, TRUE),
                        ('us_ma', 'TEST-NP-BOARD', 'CAROL TRUSTEE', 'CAROL TRUSTEE', 'Director', 'beneficial_principal', FALSE, TRUE),
                        ('us_ma', 'TEST-NP-BOARD', 'DAVE TRUSTEE', 'DAVE TRUSTEE', 'Director', 'beneficial_principal', FALSE, TRUE)
                    """
                )
            resolved = resolve_oc_company_principals(
                conn,
                [("us_ma", "TEST-NP-BOARD"), ("us_ma", "TEST-CORP-HOME")],
                max_hops=3,
            )
            np_info = resolved[("us_ma", "TEST-NP-BOARD")]
            self.assertEqual(np_info["humans"], [])
            self.assertIn("NP||550 DUDLEY STREET ROXBURY MA", np_info["hq_addresses"])
            self.assertTrue(np_info.get("is_nonprofit_isolated"))

            corp_info = resolved[("us_ma", "TEST-CORP-HOME")]
            self.assertIn("26999 SYNTHETIC MILL ROAD CHELMSFORD MA", corp_info["hq_addresses"])
            self.assertNotIn("5235 NORTH FRONT STREET HARRISBURG PA", corp_info["hq_addresses"])

            # 3) Rural Cooperative Utilities & Large >12-partner LPs must NOT export `ct_principal_ids`
            #    or `humans` into transitive clustering.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO oc_companies (
                        jurisdiction_code, company_number, name, name_norm, company_type, is_active, is_nonprofit
                    ) VALUES
                        ('us_mt', 'TEST-COOP-UTIL', 'TEST RURAL ELECTRIC CO-OP INC', 'TEST RURAL ELECTRIC CO OP INCORPORATED',
                         'Domestic Rural Cooperative Utility', TRUE, FALSE),
                        ('us_mt', 'TEST-BIG-LP', 'TEST SIXTY PARTNER LP', 'TEST SIXTY PARTNER LIMITED PARTNERSHIP',
                         'Domestic Limited Partnership', TRUE, FALSE)
                    """
                )
                for idx in range(15):
                    cur.execute(
                        """
                        INSERT INTO oc_ct_principal_links (
                            jurisdiction_code, company_number, canonical_principal_id, canonical_name,
                            pair_key, mid_init, is_homonymous_guarded, corroboration_mode, position,
                            addr_coarse, statewide_company_count, name_variants
                        ) VALUES (
                            'us_mt', 'TEST-BIG-LP', %s, %s, %s, '_', FALSE, 'statewide_unambiguous_name',
                            'partner', '', 2, ARRAY[%s]
                        )
                        """,
                        (f"us_mt:P:PARTNER{idx}|TEST:_", f"TEST PARTNER{idx}", f"PARTNER{idx}|TEST", f"TEST PARTNER{idx}"),
                    )
            resolved2 = resolve_oc_company_principals(
                conn,
                [("us_mt", "TEST-COOP-UTIL"), ("us_mt", "TEST-BIG-LP")],
                max_hops=2,
            )
            self.assertEqual(resolved2[("us_mt", "TEST-COOP-UTIL")]["humans"], [])
            self.assertEqual(resolved2[("us_mt", "TEST-COOP-UTIL")]["ct_principal_ids"], {})
            self.assertEqual(resolved2[("us_mt", "TEST-BIG-LP")]["humans"], [])
            self.assertEqual(resolved2[("us_mt", "TEST-BIG-LP")]["ct_principal_ids"], {})
        finally:
            conn.rollback()
            conn.close()

    def test_shared_charity_board_never_bridges_unrelated_business_ventures(self):
        """Nonprofits stay searchable in the system, while two unrelated private landlords
        who happen to co-serve on a charity board NEVER get bridged via BFS or Step-6 hops."""
        import os
        import psycopg2
        from api.oc_linker import cluster_with_oc_and_series, resolve_oc_company_principals

        db_url = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
        try:
            conn = psycopg2.connect(db_url, connect_timeout=3)
        except Exception:
            conn = psycopg2.connect("postgresql://user:password@ctdata_db:5432/ctdata", connect_timeout=3)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO oc_companies (
                        jurisdiction_code, company_number, name, name_norm, company_type, is_active, is_nonprofit,
                        registered_address_raw, mailing_address_raw
                    ) VALUES
                        ('us_ma', 'TEST-CHARITY-BRD', 'METROPOLITAN RELIEF HOUSING INC', 'METROPOLITAN RELIEF HOUSING INCORPORATED',
                         'Nonprofit Corporation', TRUE, TRUE, '777 CHARITY PLAZA, BOSTON, MA 02116', '777 CHARITY PLAZA, BOSTON, MA 02116'),
                        ('us_ma', 'TEST-VENTURE-A', 'ALPHA WINN APARTMENTS LLC', 'ALPHA WINN APARTMENTS LIMITED LIABILITY COMPANY',
                         'Domestic Limited Liability Company', TRUE, FALSE, '100 ALPHA WAY, BOSTON, MA 02110', '100 ALPHA WAY, BOSTON, MA 02110'),
                        ('us_ma', 'TEST-VENTURE-B', 'BETA THERESA VENTURES LLC', 'BETA THERESA VENTURES LIMITED LIABILITY COMPANY',
                         'Domestic Limited Liability Company', TRUE, FALSE, '200 BETA AVE, CAMBRIDGE, MA 02139', '200 BETA AVE, CAMBRIDGE, MA 02139')
                    """
                )
                cur.execute(
                    """
                    INSERT INTO oc_officers (
                        jurisdiction_code, company_number, name, name_norm,
                        position, role_class, is_corporate_officer, is_current
                    ) VALUES
                        ('us_ma', 'TEST-CHARITY-BRD', 'ARTHUR WINNSTON', 'ARTHUR WINNSTON', 'Director', 'beneficial_principal', FALSE, TRUE),
                        ('us_ma', 'TEST-CHARITY-BRD', 'TERESA CALCUTTA', 'TERESA CALCUTTA', 'Director', 'beneficial_principal', FALSE, TRUE),
                        ('us_ma', 'TEST-VENTURE-A', 'ARTHUR WINNSTON', 'ARTHUR WINNSTON', 'Manager', 'beneficial_principal', FALSE, TRUE),
                        ('us_ma', 'TEST-VENTURE-B', 'TERESA CALCUTTA', 'TERESA CALCUTTA', 'Manager', 'beneficial_principal', FALSE, TRUE)
                    """
                )
            resolved = resolve_oc_company_principals(
                conn,
                [
                    ("us_ma", "TEST-CHARITY-BRD"),
                    ("us_ma", "TEST-VENTURE-A"),
                    ("us_ma", "TEST-VENTURE-B"),
                ],
                max_hops=3,
            )
            charity_info = resolved[("us_ma", "TEST-CHARITY-BRD")]
            va_info = resolved[("us_ma", "TEST-VENTURE-A")]
            vb_info = resolved[("us_ma", "TEST-VENTURE-B")]

            # Charity stays in system with isolated NP|| HQ and ZERO exported board humans
            self.assertEqual(charity_info["humans"], [])
            self.assertTrue(charity_info.get("is_nonprofit_isolated"))
            self.assertIn("NP||777 CHARITY PLAZA BOSTON MA", charity_info["hq_addresses"])
            # Each private venture keeps ONLY its own manager — zero cross-contamination!
            self.assertEqual(va_info["humans"], ["ARTHUR WINNSTON"])
            self.assertEqual(vb_info["humans"], ["TERESA CALCUTTA"])

            props = [
                {"bbl": "CH-1", "owner_name": "METROPOLITAN RELIEF HOUSING INC", "owner_name_norm": "METROPOLITAN RELIEF HOUSING INC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "TEST-CHARITY-BRD"},
                {"bbl": "VA-1", "owner_name": "ALPHA WINN APARTMENTS LLC", "owner_name_norm": "ALPHA WINN APARTMENTS LLC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "TEST-VENTURE-A"},
                {"bbl": "VB-1", "owner_name": "BETA THERESA VENTURES LLC", "owner_name_norm": "BETA THERESA VENTURES LLC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "TEST-VENTURE-B"},
            ]
            out_g, _ = cluster_with_oc_and_series(props, {p["bbl"]: [p] for p in props}, resolved)
            clusters = [frozenset(p["bbl"] for p in grp) for grp in out_g.values()]
            self.assertEqual(len(clusters), 3)
            self.assertIn(frozenset({"CH-1"}), clusters)
            self.assertIn(frozenset({"VA-1"}), clusters)
            self.assertIn(frozenset({"VB-1"}), clusters)
        finally:
            conn.rollback()
            conn.close()


class TestAssessorPlaceholderAndInstitutionalHygiene(unittest.TestCase):
    def test_illinois_land_trusts_cook_placeholders_and_massgis_inverted_towns(self):
        from api.populate_cities import is_generic_owner_name, is_public_entity
        from api.shared_utils import is_excluded_third_party_entity

        for placeholder in (
            "TAXPAYER OF",
            "TAX PAYER OF",
            "TAXPAYER OF UNIT 2",
            "EXEMPT STATUS PENDING",
            "CONDO MASTER RECORD",
            "CONDO MAIN",
            "CONDO COMPLEX",
            "RESIDENT",
            "STREET",
        ):
            self.assertTrue(is_generic_owner_name(placeholder), placeholder)
            self.assertTrue(is_public_entity(placeholder), placeholder)

        for third_party in (
            "CHICAGO TITLE LAND TRU",
            "BANK FINANCIAL NA TRUS",
            "MARQUETTE BANK LAND TR",
            "CHICAGO HSING AUTHORTI",
            "CHICAGO TRANSIT AUTHOR",
            "BARNSTABLE, TOWN OF (CON)",
            "FITCHBURG, CITY OF",
            "MASS BAY TRANS AUTH",
            "TRS UNIV OF PENN",
        ):
            self.assertTrue(is_excluded_third_party_entity(third_party), third_party)

        for real_landlord in (
            "ARNOLD BANKS",
            "WEST TOWN HOUSING",
            "GEMINI TOWN HOMES LLC",
            "TAXPAYER MANAGEMENT",
            "N A ORTIZ TRUST",
            "NA KANAARIS TRUST",
            "FPACP3 TERRAZIO LLC",
        ):
            self.assertFalse(is_public_entity(real_landlord), real_landlord)

        from api.populate_cities import is_public_or_generic_address
        for junk_addr in ("CA", "VA", "MAIN", "A A", "0", "3", "#"):
            self.assertTrue(is_public_or_generic_address(junk_addr), junk_addr)
        self.assertFalse(is_public_or_generic_address("4751 WILSHIRE BLVD STE 110 LOS ANGELES CA 90010"))

    def test_bq_ct_intermediary_exchange_lender_guard_and_homonym_pid_isolation(self):
        from api.oc_linker import _INTERMEDIARY_EXCHANGE_LENDER_RE, cluster_with_oc_and_series

        for intermed in (
            "API STEWART HOLDINGS LLC",
            "NATIONAL SAFE HARBOR EXCHANGES INC",
            "AMERICAN LENDING CENTER LLC",
            "FOUNDATION FOR AFFORDABLE HOUSING V INC",
            "FIRST AMERICAN EXCHANGE COMPANY LLC",
        ):
            self.assertTrue(_INTERMEDIARY_EXCHANGE_LENDER_RE.search(intermed), intermed)

        for real_op in (
            "HANKEY FINANCE COMPANY, INC.",
            "GLEIBERMAN INVESTMENTS INC",
            "WINNCOMPANIES",
            "THE HAMILTON COMPANY, INC.",
        ):
            self.assertFalse(_INTERMEDIARY_EXCHANGE_LENDER_RE.search(real_op), real_op)

        # Two empirically homonymous DAVID KIMs with distinct BigQuery PHQ IDs must NEVER merge,
        # while two DON ROBERT HANKEY entities with the same statewide-unambiguous ID DO merge.
        props = [
            {"bbl": "B1", "owner_name": "ALPHA KIM LLC", "owner_name_norm": "ALPHA KIM LLC", "oc_jurisdiction_code": "us_ca", "oc_company_number": "C1"},
            {"bbl": "B2", "owner_name": "BETA KIM LLC", "owner_name_norm": "BETA KIM LLC", "oc_jurisdiction_code": "us_ca", "oc_company_number": "C2"},
            {"bbl": "B3", "owner_name": "SMHC 23 LLC", "owner_name_norm": "SMHC 23 LLC", "oc_jurisdiction_code": "us_ca", "oc_company_number": "C3"},
            {"bbl": "B4", "owner_name": "SEPULVEDA TOWER LLC", "owner_name_norm": "SEPULVEDA TOWER LLC", "oc_jurisdiction_code": "us_ca", "oc_company_number": "C4"},
        ]
        groups = {p["bbl"]: [p] for p in props}
        oc_resolved = {
            ("us_ca", "C1"): {
                "humans": ["DAVID KIM"],
                "ct_principal_ids": {"DAVID KIM": "us_ca:PHQ:DAVID|KIM:_@2000 WINSTON|92806"},
                "holding_entities": [],
                "reached_companies": ["us_ca/C1"],
                "hq_addresses": ["2000 WINSTON ROAD ANAHEIM CA"],
                "raw_hq_addresses": ["2000 WINSTON RD, ANAHEIM, CA 92806"],
            },
            ("us_ca", "C2"): {
                "humans": ["DAVID KIM"],
                "ct_principal_ids": {"DAVID KIM": "us_ca:PHQ:DAVID|KIM:_@7959 ALABAMA|91304"},
                "holding_entities": [],
                "reached_companies": ["us_ca/C2"],
                "hq_addresses": ["7959 ALABAMA AVENUE CANOGA PARK CA"],
                "raw_hq_addresses": ["7959 ALABAMA AVE, CANOGA PARK, CA 91304"],
            },
            ("us_ca", "C3"): {
                "humans": ["DON ROBERT HANKEY"],
                "ct_principal_ids": {"DON ROBERT HANKEY": "us_ca:P:DON|HANKEY:R"},
                "holding_entities": [],
                "reached_companies": ["us_ca/C3"],
                "hq_addresses": [],
                "raw_hq_addresses": [],
            },
            ("us_ca", "C4"): {
                "humans": ["DON ROBERT HANKEY"],
                "ct_principal_ids": {"DON ROBERT HANKEY": "us_ca:P:DON|HANKEY:R"},
                "holding_entities": [],
                "reached_companies": ["us_ca/C4"],
                "hq_addresses": [],
                "raw_hq_addresses": [],
            },
        }
        clustered, merges = cluster_with_oc_and_series(props, groups, oc_resolved)
        # B3 & B4 merged (Don Hankey); B1 & B2 stayed strictly separated (distinct David Kims)
        self.assertEqual(len(clustered), 3)
        self.assertEqual(merges, 1)
        by_bbl_group = {p["bbl"]: len(grp) for grp in clustered.values() for p in grp}
        self.assertEqual(by_bbl_group["B1"], 1)
        self.assertEqual(by_bbl_group["B2"], 1)
        self.assertEqual(by_bbl_group["B3"], 2)
        self.assertEqual(by_bbl_group["B4"], 2)

    def test_condo_trailer_tax_escrow_tribal_and_streetless_floor_hygiene(self):
        from api.shared_utils import (
            is_excluded_third_party_address,
            is_excluded_third_party_entity,
            normalize_mailing_address,
        )
        from api.populate_cities import (
            is_public_entity,
            is_public_or_generic_address,
            run_union_find_clustering,
        )

        # 1. Streetless floor/suite stubs & ServiceLink 1000 Gamma Dr rejected everywhere
        for bad_addr in (
            "03RD FL, PHILADELPHIA PA, 19143",
            "2ND FLOOR, NEWARK NJ 07102",
            "STE 400, CHICAGO IL 60601",
            "1000 GAMMA DR 3RD FLOOR, PITTSBURGH, PA, 15238",
        ):
            self.assertTrue(is_excluded_third_party_address(bad_addr), bad_addr)
            self.assertTrue(is_public_or_generic_address(bad_addr, raw_address=bad_addr), bad_addr)

        # 2. Trailer-park / lot unit markers normalize to '#<num>' so multi-unit guard separates residents
        self.assertIn("#105", normalize_mailing_address("2224 US HIGHWAY 87 E TRLR 105, BILLINGS, MT 59101"))
        self.assertIn("#42", normalize_mailing_address("500 OAK RD LOT 42, BOISE, ID 83702"))

        # 3. Sovereign tribal/federal trust land & bare street-address condo placeholders excluded
        for non_landlord in (
            "USA IN TRUST",
            "CONFEDERATED SALISH & KOOTENAI TRIBES",
            "ASSINIBOINE & SIOUX TRIBES OF THE FORT PECK INDIAN RESERVATION",
            "211 NORTH HARBOR DRIVE",
            "NEW DIRECTION IRA INC",
            "PARKWAY BK FACILITIES",
            "IMPERIAL PARKING (US) LLC",
            "LAKE COUNTY FOREST PRESERVE DISTRICT",
            "BD OF ED BARRNGTON COM UNIT SCH DIST 220",
            "STATE BANK OF THE LAKES",
            "CHGO TR BEV-2925",
            "CHICAGO LAND TRUST CO N A TTEE SBL 4670",
            "Rochester Hsg Authority",
            "MILFORD BUSINESS CENTER I ASSOCIATION, INC.",
            "NOROTON YACHT CLUB INCORPORATED",
            "RECTORY SCHOOL, INCORPORATED, THE",
            "FAIRFIELD COUNTRY DAY SCHOOL CORPORATION, THE",
            "BETHLEHEM FAIR SOCIETY, INCORPORATED, THE",
            "THIRD STONE RIDGE COOPERATIVE CORPORATION",
            "HARBORSIDE OWNERS CORPORATION",
            "NATIONAL SAFE HARBOR EXCHANGES, INC.",
            "Red Stone Equity Manager, LLC",
            "GLENDOWER MCCONAUGHY TERRACE 9% LLC",
            "THE COMMUNITY BUILDERS, INC.",
        ):
            self.assertTrue(is_public_entity(non_landlord), non_landlord)
        self.assertTrue(is_excluded_third_party_entity("USA IN TRUST"))
        self.assertTrue(is_excluded_third_party_address("1070 W CENTURY DR #101 LOUISVILLE CO 80027"))
        self.assertTrue(is_excluded_third_party_address("ONE CVS DR, WOONSOCKET, RI 02895"))
        self.assertTrue(is_excluded_third_party_address("6850 MILLER RD, BRECKSVILLE, OH 44141"))
        self.assertTrue(is_excluded_third_party_address("14806 DETROIT AVE, LAKEWOOD, OH 44107"))
        self.assertTrue(is_excluded_third_party_address("10 S LA SALLE ST, CHICAGO, IL 60603"))
        self.assertTrue(is_excluded_third_party_address("80 STATE STREET, ALBANY, NY 12207"))
        self.assertFalse(is_public_entity("211 NORTH HARBOR DRIVE LLC"))

        # 3b. Cross-state Tier 2 bare-name safety guard: Woodbury, NY business MUST NOT
        #     bare-name match an unrelated domestic North Haven, CT LLC of the same name,
        #     while a Monsey, NY landlord's CT LLC whose SOTS address IS Monsey, NY still matches.
        from api.oc_linker import OCCandidate, demangle_assessor_owner, score_oc_candidates

        ny_rec = demangle_assessor_owner(
            "KINGS HIGHWAY HOLDINGS, LLC",
            mailing_address="7600 JERICHO TURNPIKE, WOODBURY, NY, 11797",
            explicit_mailing_state="NY",
        )
        ct_homonym = OCCandidate(
            jurisdiction_code="us_ct",
            company_number="3278909",
            name="Kings HIghway Holdings, LLC",
            name_norm="KINGS HIGHWAY HOLDINGS",
            is_active=True,
            registered_address_coarse="66 KINGS HIGHWAY NORTH HAVEN CT",
            mailing_address_coarse="66 KINGS HIGHWAY NORTH HAVEN CT",
            agent_address_coarse="",
        )
        self.assertIsNone(
            score_oc_candidates(
                ny_rec,
                parcel_state="CT",
                parcel_mailing_coarse="7600 JERICHO TURNPIKE WOODBURY NY",
                candidates=[ct_homonym],
            )
        )
        ny_landlord_rec = demangle_assessor_owner(
            "BG13 PROPERTIES, LLC",
            mailing_address="8 CARTER LN, MONSEY, NY, 10952",
            explicit_mailing_state="NY",
        )
        ct_same_owner = OCCandidate(
            jurisdiction_code="us_ct",
            company_number="1111155",
            name="BG13 PROPERTIES, LLC",
            name_norm="BG13 PROPERTIES",
            is_active=True,
            registered_address_coarse="8 CARTER LANE MONSEY NY",
            mailing_address_coarse="PO BOX 936 MONSEY NY",
            agent_address_coarse="",
        )
        self.assertIsNotNone(
            score_oc_candidates(
                ny_landlord_rec,
                parcel_state="CT",
                parcel_mailing_coarse="8 CARTER LANE MONSEY NY",
                candidates=[ct_same_owner],
            )
        )

        # 3c. CT dispersed intermediary hub detector: catches PM / commercial office hubs
        #     (15 Hosley Ave: 42/197 owning, 8 islands, 36% top1; 50 Fitch St: 15/169 owning, 6 islands;
        #     15 Maiden Ln: 8/330 owning) while preserving genuine landlord HQs (623 Ellsworth: 29/43, 100%;
        #     8 Carter Ln: 16/94, 4 islands, 53%; 20 Virginia Dr: 9/79, 1 island, 100%).
        import os as _os, sys as _sys
        _api_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "api")
        if _api_dir not in _sys.path:
            _sys.path.insert(0, _api_dir)
        from api.network_builder import _is_dispersed_intermediary_business_hub

        def _make_hub_graph(island_sizes):
            g, n2i, bids = {}, {}, []
            idx = 1
            for isl_i, sz in enumerate(island_sizes):
                p_node = idx
                idx += 1
                g[p_node] = set()
                for j in range(sz):
                    bid = f"B_{isl_i}_{j}"
                    bids.append(bid)
                    b_node = idx
                    idx += 1
                    n2i[("business", bid)] = b_node
                    g[b_node] = {p_node}
                    g[p_node].add(b_node)
            return bids, g, n2i

        # 15 Hosley Ave (PM hub: 42 owning / 197 registered, 8 islands, top1=36%) -> TRUE
        b1, g1, n1 = _make_hub_graph([15, 6, 5, 4, 4, 3, 3, 2])
        self.assertTrue(_is_dispersed_intermediary_business_hub(b1, g1, n1, total_registered_at_coarse=197))
        # 50 Fitch St (office building: 15 owning / 169 registered, 6 islands) -> TRUE
        b2, g2, n2 = _make_hub_graph([10, 1, 1, 1, 1, 1])
        self.assertTrue(_is_dispersed_intermediary_business_hub(b2, g2, n2, total_registered_at_coarse=169))
        # 2083 Main St (office building: 2 owning / 23 registered, 2 islands) -> TRUE
        b3, g3, n3 = _make_hub_graph([1, 1])
        self.assertTrue(_is_dispersed_intermediary_business_hub(b3, g3, n3, total_registered_at_coarse=23))
        # 623 Ellsworth Ave (real landlord HQ: 29 owning / 43 registered, 1 island) -> FALSE
        b4, g4, n4 = _make_hub_graph([29])
        self.assertFalse(_is_dispersed_intermediary_business_hub(b4, g4, n4, total_registered_at_coarse=43))
        # 8 Carter Ln (out-of-state landlord HQ: 16 owning / 94 registered, 4 islands, 53% top1) -> FALSE
        b5, g5, n5 = _make_hub_graph([9, 3, 2, 2])
        self.assertFalse(_is_dispersed_intermediary_business_hub(b5, g5, n5, total_registered_at_coarse=94))
        # 20 Virginia Dr (single-operator office: 9 owning / 79 registered, 1 island, 100%) -> FALSE
        b6, g6, n6 = _make_hub_graph([9])
        self.assertFalse(_is_dispersed_intermediary_business_hub(b6, g6, n6, total_registered_at_coarse=79))

        # 4. Diverse-individual-homeowner mail-hub entropy guard:
        #    10 unrelated individual surnames at one address MUST NOT merge,
        #    while 4 same-surname family members at one address STILL merge cleanly!
        unrelated = [
            {
                "bbl": f"U{i}",
                "address": f"{100 + i} OAK ST",
                "borough": "CLEVELAND (CUYAHOGA)",
                "zip_code": "44107",
                "owner_name": nm,
                "mailing_address": "555 SHARED CONDO TOWERS AVE, LAKEWOOD, OH 44107",
            }
            for i, nm in enumerate([
                "ACKER, KATHERINE", "ADAM, JILL", "AHMED, MARGARET", "ALJASSABI, AHMAD",
                "ALLEN, JULIA", "ALSTON, RETONYA", "AMPONSAH, KOFI", "ARMSTRONG, NICHOLAS",
                "AVERRE, KATHLEEN", "BABCOCK, GARRETT",
            ])
        ]
        family = [
            {
                "bbl": f"F{i}",
                "address": f"{200 + i} ELM ST",
                "borough": "CLEVELAND (CUYAHOGA)",
                "zip_code": "44107",
                "owner_name": nm,
                "mailing_address": "88 FAMILY HQ BLVD, LAKEWOOD, OH 44107",
            }
            for i, nm in enumerate([
                "GUREVITCH, MENACHEM", "GUREVITCH, DOV", "GUREVITCH, SHLOMO", "GUREVITCH, MIRIAM",
            ])
        ]
        groups = run_union_find_clustering(unrelated + family, "oh")
        # 10 unrelated owners stay 10 separate networks + 1 unified family network = 11 groups!
        self.assertEqual(len(groups), 11)
        fam_sizes = [len(g) for g in groups.values() if any(p["bbl"].startswith("F") for p in g)]
        self.assertEqual(fam_sizes, [4])

    def test_universal_address_onsite_trust_and_nominee_manager_edge_cases(self):
        from api.shared_utils import (
            extract_postal_street_zip_key,
            is_excluded_third_party_entity,
            normalize_mailing_address_coarse,
        )
        from api.populate_cities import is_generic_owner_name, run_union_find_clustering
        from api.oc_linker import _INTERMEDIARY_EXCHANGE_LENDER_RE, cluster_with_oc_and_series

        # 1. Universal address-normalization & ZIP-key collision fixes:
        #    lettered avenues, numbered highways/county roads, ST->SAINT, MOUNT, double suffixes, quadrants
        distinct_pairs = [
            ("10530 S AVENUE E, CHICAGO, IL 60617", "10530 S AVENUE F, CHICAGO, IL 60617"),
            ("100 AVENUE A, NEW YORK, NY 10009", "100 AVENUE B, NEW YORK, NY 10009"),
            ("100 US HIGHWAY 2, KALISPELL, MT 59901", "100 US HIGHWAY 93, KALISPELL, MT 59901"),
            ("100 COUNTY ROAD 12, DULUTH, MN 55803", "100 COUNTY ROAD 42, DULUTH, MN 55803"),
            ("100 MOUNT VERNON ST, BOSTON, MA 02108", "100 MOUNT PLEASANT ST, BOSTON, MA 02108"),
            ("100 ST CHARLES AVE, NEW ORLEANS, LA 70130", "100 ST JOSEPH ST, NEW ORLEANS, LA 70130"),
            ("100 COURT STREET, BOSTON, MA 02108", "100 COURT SQUARE, BOSTON, MA 02108"),
            ("1200 K ST NW, WASHINGTON, DC 20005", "1200 K ST NE, WASHINGTON, DC 20005"),
            ("100 PARK AVE S, NEW YORK, NY 10003", "100 PARK AVE N, NEW YORK, NY 10003"),
        ]
        for a, b in distinct_pairs:
            self.assertNotEqual(normalize_mailing_address_coarse(a), normalize_mailing_address_coarse(b), (a, b))
            za, zb = extract_postal_street_zip_key(a), extract_postal_street_zip_key(b)
            self.assertTrue(za != zb or za == "", (a, b, za, zb))
        # Neighborhood/abbreviation unification still works on valid same-building addresses
        self.assertEqual(
            extract_postal_street_zip_key("39 BRIGHTON AVE, ALLSTON, MA 02134"),
            extract_postal_street_zip_key("39 BRIGHTON AV, BOSTON, MA 02134-2013"),
        )

        # 2. Both-onsite 2-4 unit condo/duplex neighbors NEVER merge across distinct owners,
        #    while a landlord living/officing onsite at 100 MAIN ST still merges with their offsite rental.
        onsite_test_props = [
            {
                "bbl": "C1", "address": "4709 CHURCH ST B", "borough": "SKOKIE", "zip_code": "60076",
                "owner_name": "CICILYN WALFORD", "mailing_address": "4709 CHURCH ST B, SKOKIE, IL 60076",
            },
            {
                "bbl": "C2", "address": "4709 CHURCH ST # D", "borough": "SKOKIE", "zip_code": "60076",
                "owner_name": "COMMERCIAL ACQUISITIONS LLC", "mailing_address": "4709 CHURCH ST # D, SKOKIE, IL 60076",
            },
            {
                "bbl": "L1", "address": "100 MAIN ST", "borough": "SKOKIE", "zip_code": "60076",
                "owner_name": "100 MAIN LLC", "mailing_address": "100 MAIN ST, SKOKIE, IL 60076",
            },
            {
                "bbl": "L2", "address": "200 ELM ST", "borough": "SKOKIE", "zip_code": "60076",
                "owner_name": "200 ELM LLC", "mailing_address": "100 MAIN ST, SKOKIE, IL 60076",
            },
        ]
        ogroups = run_union_find_clustering(onsite_test_props, "il")
        by_b = {p["bbl"]: root for root, grp in ogroups.items() for p in grp}
        self.assertNotEqual(by_b["C1"], by_b["C2"])
        self.assertEqual(by_b["L1"], by_b["L2"])

        # 3. Truncated generic trust placeholders & newly widened universal non-landlord patterns
        for placeholder in ("TRUST AGREEMENT DATED", "DECLARATION OF TRUST O", "TRUSTEE COMPANY LAND TRUST LLC"):
            self.assertTrue(is_generic_owner_name(placeholder), placeholder)
        self.assertFalse(is_generic_owner_name("SMITH FAMILY REVOCABLE LIVING TRUST"))
        for non_landlord in (
            "CRESTWOOD VIG CO-OP IV INC", "ROLLING MEADOWS COOP", "EQUITY TRUST CUSTODIAN",
            "CHICAGO TRUST COMPANY", "VERIZON NEW ENGLAND INC", "Virginia Commonwealth Univ Academic Division",
            "UNITED COMMUNITY HOUSING COALITION", "POTLATCHDELTIC TIMBER LLC", "KWIK TRIP INC",
        ):
            self.assertTrue(is_excluded_third_party_entity(non_landlord), non_landlord)
        self.assertFalse(is_excluded_third_party_entity("100 UNIV ST PROPERTIES LLC"))
        self.assertTrue(_INTERMEDIARY_EXCHANGE_LENDER_RE.search("JR MANAGER SERVICES LLC"))
        self.assertTrue(_INTERMEDIARY_EXCHANGE_LENDER_RE.search("FLORIDA CORPORATE GROUP LLC"))

        # 4. Structural nominee-manager dispersion guard in `cluster_with_oc_and_series`:
        #    A corporate manager atop 10 unrelated client LLCs with 10 distinct HQs MUST NOT merge them,
        #    while a real landlord GP atop 10 SPVs sharing 1 executive HQ STILL merges all 10!
        client_props = [
            {
                "bbl": f"CL{i}", "owner_name": f"CLIENT{i} HOLDINGS LLC",
                "owner_name_norm": f"CLIENT{i} HOLDINGS LLC",
                "mailing_address": f"{100 + i} UNIQUE CLIENT BLVD, MIAMI, FL 33131",
                "_coarse_mail": f"{100 + i} UNIQUE CLIENT BOULEVARD MIAMI FL",
                "oc_jurisdiction_code": "us_fl", "oc_company_number": f"CL{i}",
            }
            for i in range(10)
        ]
        gp_props = [
            {
                "bbl": f"GP{i}", "owner_name": f"PORTFOLIO{i} ASSET LLC",
                "owner_name_norm": f"PORTFOLIO{i} ASSET LLC",
                "mailing_address": "500 SINGLE LANDLORD HQ AVE, MIAMI, FL 33131",
                "_coarse_mail": "500 SINGLE LANDLORD HQ AVENUE MIAMI FL",
                "oc_jurisdiction_code": "us_fl", "oc_company_number": f"GP{i}",
            }
            for i in range(10)
        ]
        all_p = client_props + gp_props
        init_g = {p["bbl"]: [p] for p in all_p}
        oc_res = {}
        for i in range(10):
            oc_res[("us_fl", f"CL{i}")] = {
                "humans": [], "holding_entities": ["VESTA NOMINEE MANAGER LLC"],
                "reached_companies": [f"us_fl/CL{i}", "us_fl/NOMINEE_FACTORY"],
                "hq_addresses": [], "raw_hq_addresses": [],
            }
            oc_res[("us_fl", f"GP{i}")] = {
                "humans": [], "holding_entities": ["REAL LANDLORD GP LLC"],
                "reached_companies": [f"us_fl/GP{i}", "us_fl/REAL_GP"],
                "hq_addresses": [], "raw_hq_addresses": [],
            }
        out_g, _ = cluster_with_oc_and_series(all_p, init_g, oc_res)
        cl_roots = {root for root, grp in out_g.items() for p in grp if p["bbl"].startswith("CL")}
        gp_roots = {root for root, grp in out_g.items() for p in grp if p["bbl"].startswith("GP")}
        self.assertEqual(len(cl_roots), 10)  # 10 unrelated clients stayed strictly separate!
        self.assertEqual(len(gp_roots), 1)   # 10 real landlord SPVs unified into 1 portfolio!

    def test_mother_teresa_nonprofit_firewall_non_beneficial_roles_and_generic_manager(self):
        from api.oc_linker import (
            _EXCLUDED_OC_COMPANY_TYPE_RE,
            _NONPROFIT_BOARD_ISOLATED_TYPE_RE,
            _NON_BENEFICIAL_OFFICER_POSITION_RE,
            cluster_with_oc_and_series,
            is_generic_homonym_stem,
        )
        from api.network_builder import _EXCLUDED_CT_BIZ_TYPE_RE, _NONPROFIT_BOARD_ISOLATED_CT_TYPE_RE

        # Generic bare corporate names must never Tier-2 match across unrelated entities
        for generic_nm in ("MANAGER LLC", "MANAGING LLC", "OWNER LLC", "TRUSTEE LLC", "GENERAL PARTNER LP"):
            self.assertTrue(is_generic_homonym_stem(generic_nm), generic_nm)
        self.assertFalse(is_generic_homonym_stem("WINNCOMPANIES LLC"))

        # Nonprofit / Non-Stock / Charitable / Public-Benefit types are board-isolated (NOT omitted!)
        for non_profit_ct in (
            "Non-Profit Corporation - Domestic",
            "Nonprofit Corporation",
            "Non-Stock",
            "DOMESTIC NOT-FOR-PROFIT CORPORATION",
            "DOMESTIC NOT-FOR-PROFIT CORPORATION (HOUSING DEVELOPMENT FUND COMPANY) (ARTICLE XI)",
            "Public Benefit Corporation",
            "Mutual Benefit Nonprofit Corporation",
            "Charitable Corporation",
        ):
            self.assertTrue(bool(_NONPROFIT_BOARD_ISOLATED_TYPE_RE.search(non_profit_ct)), non_profit_ct)
        for non_landlord_ct in (
            "DOMESTIC PROFESSIONAL SERVICE CORPORATION",
            "DOMESTIC PROFESSIONAL SERVICE LIMITED LIABILITY COMPANY",
            "Religious",
        ):
            self.assertTrue(bool(_EXCLUDED_OC_COMPANY_TYPE_RE.search(non_landlord_ct)), non_landlord_ct)
        for ct_np in (
            "Non-Stock",
            "DOMESTIC NOT-FOR-PROFIT CORPORATION",
            "DOMESTIC NOT-FOR-PROFIT CORPORATION (COMMUNITY MENTAL HEALTH SERVICES AND MENTAL RETARDATION SERVICES COMPANIES)",
        ):
            self.assertTrue(bool(_NONPROFIT_BOARD_ISOLATED_CT_TYPE_RE.search(ct_np)), ct_np)
        for ct_excl in ("DOMESTIC PROFESSIONAL SERVICE CORPORATION", "DOMESTIC COOPERATIVE CORPORATION", "Religious"):
            self.assertTrue(bool(_EXCLUDED_CT_BIZ_TYPE_RE.search(ct_excl)), ct_excl)
        for ct_allowed in ("LLC", "Stock", "DOMESTIC LIMITED LIABILITY COMPANY", "DOMESTIC BUSINESS CORPORATION", "Limited Partnership"):
            self.assertFalse(bool(_EXCLUDED_CT_BIZ_TYPE_RE.search(ct_allowed)), ct_allowed)
            self.assertFalse(bool(_NONPROFIT_BOARD_ISOLATED_CT_TYPE_RE.search(ct_allowed)), ct_allowed)

        # Volunteer / advisory / clerical / religious / agent officer positions excluded
        for bad_pos in ("Advisory Board", "Honorary Director", "Chaplain", "Assistant Secretary", "Registered Agent", "Attorney", "Volunteer"):
            self.assertTrue(bool(_NON_BENEFICIAL_OFFICER_POSITION_RE.search(bad_pos)), bad_pos)
        for good_pos in ("Manager", "Managing Member", "President", "Beneficial Owner", "General Partner"):
            self.assertFalse(bool(_NON_BENEFICIAL_OFFICER_POSITION_RE.search(good_pos)), good_pos)

        # Nonprofit housing operators STAY in `*_networks` as their own isolated portfolios,
        # while NEVER bridging into private landlords (even at a co-located street address!)
        # and exporting ZERO volunteer board members into BFS clustering.
        props = [
            {"bbl": "NP1", "owner_name": "DUDLEY NEIGHBORS INC", "owner_name_norm": "DUDLEY NEIGHBORS INC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "NP_1"},
            {"bbl": "NP2", "owner_name": "DUDLEY NEIGHBORS TWO INC", "owner_name_norm": "DUDLEY NEIGHBORS TWO INC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "NP_2"},
            {"bbl": "PR1", "owner_name": "PRIVATE LANDLORD ALPHA LLC", "owner_name_norm": "PRIVATE LANDLORD ALPHA LLC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "PR_1"},
            {"bbl": "PR2", "owner_name": "PRIVATE LANDLORD BETA LLC", "owner_name_norm": "PRIVATE LANDLORD BETA LLC", "oc_jurisdiction_code": "us_ma", "oc_company_number": "PR_2"},
        ]
        init_g = {p["bbl"]: [p] for p in props}
        oc_res = {
            ("us_ma", "NP_1"): {"humans": [], "holding_entities": [], "reached_companies": ["us_ma/NP_1"], "hq_addresses": ["NP||100 MAIN STREET BOSTON MA"], "raw_hq_addresses": ["100 MAIN ST, BOSTON, MA"], "is_nonprofit_isolated": True},
            ("us_ma", "NP_2"): {"humans": [], "holding_entities": [], "reached_companies": ["us_ma/NP_2"], "hq_addresses": ["NP||100 MAIN STREET BOSTON MA"], "raw_hq_addresses": ["100 MAIN ST, BOSTON, MA"], "is_nonprofit_isolated": True},
            ("us_ma", "PR_1"): {"humans": ["ALICE LANDLORD"], "holding_entities": [], "reached_companies": ["us_ma/PR_1"], "hq_addresses": ["100 MAIN STREET BOSTON MA"], "raw_hq_addresses": ["100 MAIN ST, BOSTON, MA"], "is_nonprofit_isolated": False},
            ("us_ma", "PR_2"): {"humans": ["BOB LANDLORD"], "holding_entities": [], "reached_companies": ["us_ma/PR_2"], "hq_addresses": ["200 OAK STREET BOSTON MA"], "raw_hq_addresses": ["200 OAK ST, BOSTON, MA"], "is_nonprofit_isolated": False},
        }
        out_g, _ = cluster_with_oc_and_series(props, init_g, oc_res)
        clusters = [frozenset(p["bbl"] for p in grp) for grp in out_g.values()]
        self.assertIn(frozenset({"NP1", "NP2"}), clusters)  # Sibling nonprofits unify with each other
        self.assertIn(frozenset({"PR1"}), clusters)          # Private landlord at same address NEVER merges with nonprofit
        self.assertIn(frozenset({"PR2"}), clusters)          # Unrelated private landlord stays completely separate
        self.assertEqual(len(clusters), 3)


if __name__ == "__main__":
    unittest.main()
