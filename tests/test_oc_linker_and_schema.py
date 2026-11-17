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


if __name__ == "__main__":
    unittest.main()
