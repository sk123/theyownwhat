import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.populate_cities import run_union_find_clustering
from api.shared_utils import (
    extract_owner_family_key,
    get_email_match_key,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
    normalize_mailing_address_coarse,
    strip_benign_care_of_prefix,
)


class ThirdPartyExclusionTests(unittest.TestCase):
    def test_excludes_banks_trustees_servicers_ras_and_institutions(self):
        excluded_names = [
            "CITIBANK, N.A.",
            "CITIBANK NA AS TRUSTEE FOR CMLTI ASSET TRUST",
            "CITIGROUP MORTGAGE LOAN TRUST INC.",
            "CITIMORTGAGE INC",
            "CITIGROUP TECHNOLOGY, INC.",
            "WILMINGTON SAVINGS FUND SOCIETY, FSB",
            "WILMINGTON SAVING FUND SOCIETY FSB",
            "WILMINGTON SAVNGS FUND SOCIETY",
            "BRANCH BANKING AND TRUST COMPANY",
            "BANKERS TRUST COMPANY OF SOUTH DAKOTA, TRUSTEE",
            "EQUITY TRUST COMPANY CUSTODIAN FBO JAMES ARTHUR TOWNSEND IRA",
            "ENTRUST GROUP FBO LIONEL REAVES, THE",
            "MINNEAPOLIS PUBLIC HOUSING",
            "MPHA",
            "SECURITY TRUST COMPANY TRUSTEES",
            "PROF-2014-S2 LEGAL TITLE TRUST",
            "INDUSTRIAL BANK WASHINGTON",
            "COMMONWEALTH OF",
            "DORAL CITY OF",
            "U.S. BANK TRUST NATIONAL ASSOCIATION",
            "US BANK TRUST NA AS TRUSTEE FOR LSF9 MASTER",
            "JPMORGAN CHASEBANK NATIONAL ASSOCIATION",
            "JPMORGAN CHASE BANK, N.A.",
            "WELLS FARGO BANK, NA",
            "THE BANK OF NEW YORK MELLON",
            "BANK OF NY AS TRUSTEE",
            "BANK OF AMERICA, N.A.",
            "DEUTSCHE BANK NATIONAL TRUST COMPANY",
            "HSBC BANK USA, NATIONAL ASSOCIATION",
            "KEYBANK NATIONAL ASSOCIATION",
            "CITIZENS BANK, NATIONAL ASSOCIATION",
            "FIFTH THIRD BANK, NATIONAL ASSOCIATION",
            "COMERICA BANK C/O RYAN, LLC",
            "ION BANK",
            "PNC BANK, NATIONAL ASSOCIATION",
            "TRUIST BANK",
            "FANNIE MAE",
            "FREDDIE MAC",
            "FEDERAL NATIONAL MORTGAGE ASSOCIATION",
            "MTGLQ INVESTORS, L.P.",
            "STANWICH MORTGAGE LOAN TRUST I",
            "NATIONSTAR MORTGAGE LLC",
            "NATIONSTAR MORTGAGE DBA MR COOPER",
            "PENNYMAC LOAN SERVICES, LLC",
            "FREEDOM MORTGAGE CORPORATION",
            "LAKEVIEW LOAN SERVICING LLC",
            "LOANCARE LLC",
            "ROUNDPOINT MORTGAGE SERVICING LLC",
            "CALIBER HOME LOANS INC",
            "SELECT PORTFOLIO SERVICING, INC.",
            "ONITY GROUP INC",
            "CELINK",
            "GUARDIAN ASSET MANAGEMENT",
            "REO MANAGEMENT SOLUTIONS LLC",
            "SAFEGUARD PROPERTIES MANAGEMENT LLC",
            "H & R BLOCK TAX SERV LLC",
            "CORPORATION SERVICE COMPANY",
            "C T CORPORATION SYSTEM",
            "NATIONAL REGISTERED AGENTS, INC.",
            "LEGALINC CORPORATE SERVICES INC.",
            "NORTHWEST REGISTERED AGENT SERVICE, INC.",
            "COMMWLTH OF MASS",
            "COMMONWEALTH OF MASSACHUSETTS",
            "STATE OF FLORIDA & DOT",
            "MAYOR AND CITY COUNCIL OF BALTIMORE",
            "MIAMI DADE COUNTY",
            "DETROIT HOUSING COMMISSION",
            "DETROIT LAND BANK AUTHORITY",
            "NEW YORK CITY HOUSING AUTHORITY",
            "SEPTA",
            "WMATA",
            "AMTRAK",
            "CONRAIL",
            "CSX TRANSPORTATION, INC",
            "DETROIT INTERNATIONAL BRIDGE CO",
            "BALTO. GAS &ELECTRIC CO",
            "DTE ELECTRIC COMPANY",
            "FLORIDA POWER & LIGHT CO",
            "PEPCO",
            "GENERAL HOSPITAL CORP",
            "NEWTON-WELLESLEY HOSPITAL",
            "MARYLAND HEALTH SYSTEMS, INC.",
            "GREEK ORTHODOX CHURCH ST NICHOLAS",
            "ROMAN CATH ARCH OF BOS",
            "ARKANSAS STATE HIGHWAY COMMISSION",
            "ARKANSAS STATE HIGHWAY",
            "ARK STATE HWY COMM",
            "USA",
            "HUD",
            "BOSTON REDEVELOPMNT AUTH",
            "COMMONWLTH OF MASS",
            "MASS TRANSIT ADMINISTRATION, THE",
            "MARYLAND TRANSIT ADMINISTRATION",
            "NATIONAL RAILROAD PASSENGER CORPORATION",
            "GREEN MOUNTAIN POWER CORP",
            "WEYERHAEUSER FOREST HOLDINGS INC",
            "HOT SPRINGS VILLAGE POA",
            "BVPOA",
            "FFBCCI",
            "CLEARBROOK COMMUNITY CONDO ASSN",
            "WILLIAMSBURG VILLAGE CONDOMINIUM ASSOCIATION",
            "WINTHROP CENTER RESIDENTIAL CONDOMINIUNM TRUST",
            "77 SCHOOL STREET CONDOMINIUM TRUST",
            "PAN AM SOUTHERN LLC",
            "HOME PATH FINANCIAL LP",
        ]
        for name in excluded_names:
            with self.subTest(name=name):
                self.assertTrue(
                    is_excluded_third_party_entity(name),
                    f"Expected 3rd-party exclusion for {name!r}",
                )

    def test_preserves_genuine_private_landlords_and_street_named_llcs(self):
        genuine_landlords = [
            "Menachem Gurevitch",
            "DUN SRULOWITZ",
            "Aaron Landini",
            "BURGESS HOPE",
            "SPELIOTIS ISMENE",
            "WEST BANK REALTY LLC",
            "RIVERBANK REALTY LLC",
            "RED BANK APARTMENTS LLC",
            "1628 CHURCH ST, LLC",
            "3024 CHURCH AVENUE LLC",
            "1120 COLLEGE AVE HDFC",
            "55 UNIVERSITY PL ASSOCIATES LLC",
            "77 SCHOOL STREET REALTY TRUST",
            "AVE P 380 CT CORPORATION",
            "FKH SFR PROPCO B-HLD LP",
            "IH3 PROPERTY FLORIDA LP",
            "BALTIMORE SFR PORTFOLIO I, LLC",
            "BALTIMORE EXCEL 02 LLC",
            "WILMINGTON MENLO LLC",
            "JEFFREY A BANKS TRUSTEE",
            "BARBARA BANKOFF, LIVING TRUST",
            "DEERA LLC",
            "HORNIG, DAVID",
            "CHAD D KRAFT",
        ]
        for name in genuine_landlords:
            with self.subTest(name=name):
                self.assertFalse(
                    is_excluded_third_party_entity(name),
                    f"False-positive 3rd-party exclusion on genuine landlord {name!r}",
                )

    def test_excludes_servicer_ra_pmb_and_third_party_care_of_addresses(self):
        bad_addresses = [
            "55 BEATTIE PL STE 100 GREENVILLE SC 29601",
            "3217 S DECKER LAKE DR SALT LAKE CITY UT 84119",
            "8950 CYPRESS WATERS BLVD COPPELL TX 75019",
            "3501 OLYMPUS BLVD STE 500 DALLAS TX 75019",
            "1661 WORTHINGTON RD STE 100 WEST PALM BEACH FL 33409",
            "5426 BAY CENTER DR STE 300 TAMPA FL 33609",
            "3476 STATE VIEW BLVD FORT MILL SC 29715",
            "3900 CAPITAL CITY BLVD LANSING MI 48906",
            "5000 THAYER CTR STE C OAKLAND MD 21550",
            "251 LITTLE FALLS DR WILMINGTON DE 19808",
            "712 H ST NE PMB 91443 WASHINGTON DC 20002",
            "23636 MICHIGAN AVE #539 DEARBORN MI 48124",
            "C/O GREENE LAW PC, 11 TALCOTT NOTCH RD, FARMINGTON CT 06032",
            "C/O ODESSA PROPERTY MANAGEMENT, 57 W TIMONIUM RD STE 105A, TIMONIUM MD 21093",
            "C/O RYAN, LLC, PO BOX 460049, DEPT 502, HOUSTON TX 77056",
        ]
        for raw_addr in bad_addresses:
            coarse = normalize_mailing_address_coarse(raw_addr)
            with self.subTest(raw_addr=raw_addr):
                self.assertTrue(
                    is_excluded_third_party_address(raw_addr, coarse, ()),
                    f"Expected 3rd-party address exclusion for {raw_addr!r} (coarse={coarse!r})",
                )

    def test_preserves_benign_corporate_parent_care_of_addresses(self):
        raw_ih = "C/O INVITATION HOMES - TAX DEPT, 2525 W FRYE RD STE 300, CHANDLER, AZ 85224"
        stripped_ih = strip_benign_care_of_prefix(raw_ih)
        coarse_ih = normalize_mailing_address_coarse(stripped_ih)
        self.assertEqual(coarse_ih, "2525 WEST FRYE ROAD CHANDLER AZ")
        self.assertFalse(is_excluded_third_party_address(raw_ih, coarse_ih, ()))

        raw_fkh = "C/O FIRST KEY HOMES LLC, 1850 PARKWAY PLACE STE 900, MARIETTA, GA 30067"
        stripped_fkh = strip_benign_care_of_prefix(raw_fkh)
        coarse_fkh = normalize_mailing_address_coarse(stripped_fkh)
        self.assertEqual(coarse_fkh, "1850 PARKWAY PLACE MARIETTA GA")
        self.assertFalse(is_excluded_third_party_address(raw_fkh, coarse_fkh, ()))

    def test_owner_family_key_collapses_numbered_series_spvs_only(self):
        self.assertEqual(
            extract_owner_family_key("BALTIMORE EXCEL 01"),
            extract_owner_family_key("BALTIMORE EXCEL 26"),
        )
        self.assertNotEqual(
            extract_owner_family_key("DEERA"),
            extract_owner_family_key("EGBE VENTURES"),
        )

    def test_email_match_key_blocks_banks_servicers_and_registered_agents(self):
        for email in (
            "reo@citi.com",
            "escrow@mrcooper.com",
            "filings@legalinc.com",
            "closing@titlecompany.com",
        ):
            self.assertIsNone(get_email_match_key(email, {}))

    def test_union_find_excludes_citibank_and_prevents_servicer_address_bridging(self):
        servicer_addr = "55 BEATTIE PL STE 100 GREENVILLE SC 29601"
        properties = [
            {
                "bbl": "0001-001",
                "owner_name": "CITIBANK, N.A.",
                "mailing_address": servicer_addr,
                "units_res": 2,
                "borough": "BALTIMORE",
            },
            {
                "bbl": "0001-002",
                "owner_name": "WILMINGTON SAVINGS FUND SOCIETY, FSB",
                "mailing_address": servicer_addr,
                "units_res": 1,
                "borough": "BALTIMORE",
            },
            {
                "bbl": "0002-001",
                "owner_name": "DEERA LLC",
                "mailing_address": servicer_addr,
                "units_res": 3,
                "borough": "BALTIMORE",
            },
            {
                "bbl": "0002-002",
                "owner_name": "DEERA LLC",
                "mailing_address": "100 PRIVATE OWNER LN BALTIMORE MD 21201",
                "units_res": 4,
                "borough": "BALTIMORE",
            },
            {
                "bbl": "0003-001",
                "owner_name": "EGBE VENTURES LLC",
                "mailing_address": servicer_addr,
                "units_res": 2,
                "borough": "BALTIMORE",
            },
        ]
        groups = run_union_find_clustering(properties, "baltimore")
        clusters = list(groups.values())
        all_names = {
            p["owner_name"]
            for cluster in clusters
            for p in cluster
        }
        self.assertFalse(any("CITIBANK" in n for n in all_names))
        self.assertFalse(any("WILMINGTON" in n for n in all_names))

        # DEERA LLC keeps both of its own buildings, and never merges with EGBE VENTURES LLC!
        self.assertEqual(len(clusters), 2)
        by_owner = {cluster[0]["owner_name"]: cluster for cluster in clusters}
        self.assertEqual(len(by_owner["DEERA LLC"]), 2)
        self.assertEqual(len(by_owner["EGBE VENTURES LLC"]), 1)

    def test_redacted_and_joint_placeholder_owner_names_excluded(self):
        from api.populate_cities import is_generic_owner_name

        for redacted in (
            "NOT AVAILABLE & NOT AVAILABLE",
            "Unavailable",
            "Unavailable Owner",
            "UNAVAILABLE OWNER",
            "Name Not on File",
            "NAME NOT ON FILE",
            "** CONFIDENTIAL **",
            "OWNER OF RECORD",
            "CURRENT OWNER OF",
            "Confidential Owner & Confidential Owner",
            "Confidential Owner",
            "Private Owner & Private Owner",
            "private owner",
            "UNKNOWN HEIRS OF",
            "OWNER UNKNOWN",
            "UNKNOWN, UNKNOWN",
            "Name Withheld",
            "NAME NOT AVAILABLE",
        ):
            with self.subTest(redacted=redacted):
                self.assertTrue(is_generic_owner_name(redacted))

        for real_owner in (
            "NO NAME KEY ASSETS LLC",
            "No Name Road, LLC",
            "SANDRA K KUKA & NOT AVAILABLE NOT AVAILABLE",
            "PROGRESS RESIDENTIAL BORROWER",
            "VB ONE LLC",
        ):
            with self.subTest(real_owner=real_owner):
                self.assertFalse(is_generic_owner_name(real_owner))

    def test_commercial_tax_agents_nnn_lessees_and_bare_cities_are_excluded_addresses(self):
        for bad_addr in (
            "2424 Ridge Rd, Rockwall, TX 75087",
            "6920 POINTE INVERNESS WAY STE 301, FORT WAYNE IN 46804",
            "% BADEN TAX MANAGEMENT LLC, FORT WAYNE, IN 46804",
            "% FAMILY DOLLAR STORES, CHESAPEAKE, VA 23320",
            "C/O 7-ELEVEN INC ONE ARTS PLAZA, 1722 ROUTH STREET SUITE 1000, DALLAS, TX 75201",
            "% MONRO INC ATTN: TAX DEPT, FORT WAYNE, IN 46804",
            "CHESAPEAKE, VA 23320",
            "Sussex, DE",
        ):
            with self.subTest(bad_addr=bad_addr):
                self.assertTrue(is_excluded_third_party_address(bad_addr))

        for real_hq in (
            "1025 CATHEDRAL STREET, BALTIMORE, MD, 21201",
            "829 N 29TH ST, #5, PHILADELPHIA, PA, 19130",
            "997 MORRISON DR, SUITE 402, CHARLESTON, SC, 29403",
            "PO BOX 461, MIDLOTHIAN, VA 23113",
        ):
            with self.subTest(real_hq=real_hq):
                self.assertFalse(is_excluded_third_party_address(real_hq))

    def test_crestwood_coop_and_cross_jurisdiction_sibling_series_unification(self):
        from api.oc_linker import cluster_with_oc_and_series
        from api.shared_utils import looks_like_person_owner, normalize_business_name

        # 1. CO-OP must never be mangled into COMPANY-OP, and initialisms/inversions must align
        self.assertEqual(
            normalize_business_name("CRESTWOOD VIG CO-OP IV INC"),
            "CRESTWOOD VILLAGE COOPERATIVE IV INCORPORATED",
        )
        self.assertEqual(
            normalize_business_name("D R HORTON INC"),
            normalize_business_name("D.R. HORTON, INC"),
        )
        self.assertEqual(
            normalize_business_name("THR FLORIDA LP"),
            normalize_business_name("THR FLORIDA L P"),
        )
        self.assertEqual(
            normalize_business_name("GREENBRIAR REALTY CORP"),
            normalize_business_name("GREENBRIAR REALTY CORPORATION, THE"),
        )

        # 2. Resident-owned housing co-ops (`CRESTWOOD VIG CO-OP`) are excluded as non-landlord cooperatives,
        #    while genuine private Roman/numbered series (`CRESTWOOD MANOR IV LLC`, `1`, `2`, `3`) unify into
        #    1 network and unrelated `CRESTWOOD HOLDINGS LLC` stays separate.
        self.assertTrue(is_excluded_third_party_entity("CRESTWOOD VIG CO-OP IV INC"))
        nj_props = [
            {"bbl": "nj-1", "owner_name": "CRESTWOOD MANOR IV LLC", "county": "OCEAN", "mailing_address": "15E MOCCASIN DR, WHITING, NJ, 08759"},
            {"bbl": "nj-2", "owner_name": "CRESTWOOD MANOR LLC 1", "county": "OCEAN", "mailing_address": "92 FAIRWAY LN, WHITING, NJ, 08759"},
            {"bbl": "nj-3", "owner_name": "CRESTWOOD MANOR 2 LLC", "county": "OCEAN", "mailing_address": "470 ROUTE 530, WHITING, NJ, 08759"},
            {"bbl": "nj-4", "owner_name": "CRESTWOOD MANOR 3 LLC", "county": "OCEAN", "mailing_address": "250 SCHOOLHOUSE RD, WHITING, NJ, 08759"},
            {"bbl": "nj-5", "owner_name": "CRESTWOOD HOLDINGS LLC", "county": "MIDDLESEX", "mailing_address": "112 HILLSIDE AVE, SOUTH PLAINFIELD, NJ, 07080"},
            # Cross-county generic landscape homonyms must NOT merge across Bergen vs Middlesex
            {"bbl": "nj-6", "owner_name": "NORTH VILLAGE 1 LLC", "county": "BERGEN", "mailing_address": "100 CANAL POINTE BLVD, PRINCETON, NJ, 08540"},
            {"bbl": "nj-7", "owner_name": "NORTH VILLAGE ASSOC", "county": "MIDDLESEX", "mailing_address": "911 VILLAGE DR EAST, NORTH BRUNSWICK, NJ, 08902"},
            # Base unnumbered entity + numbered series SPVs unify cleanly
            {"bbl": "md-1", "owner_name": "SHELBY COMPANY LLC", "borough": "WARD 15", "mailing_address": ""},
            {"bbl": "md-2", "owner_name": "SHELBY COMPANY 2 LLC", "borough": "WARD 15", "mailing_address": ""},
            {"bbl": "md-3", "owner_name": "SHELBY COMPANY 3 LLC", "borough": "WARD 15", "mailing_address": ""},
            # 4-digit year tranches + single-letter PROPCO tranches + truncated CAMA 'LL'
            {"bbl": "fl-1", "owner_name": "TRICON SFR 2024-4 BORROWER LLC", "borough": "HILLSBOROUGH", "mailing_address": ""},
            {"bbl": "fl-2", "owner_name": "TRICON SFR 2020-2 BORROWER LLC", "borough": "ORANGE", "mailing_address": ""},
            {"bbl": "pa-1", "owner_name": "PHILADELPHIA LOTUS 04A LLC", "borough": "PHILADELPHIA", "mailing_address": ""},
            {"bbl": "pa-2", "owner_name": "PHILADELPHIA LOTUS 03A LL", "borough": "PHILADELPHIA", "mailing_address": ""},
        ]
        initial_groups = {p["bbl"]: [p] for p in nj_props}
        merged, _ = cluster_with_oc_and_series(nj_props, initial_groups, {})
        bbl_sets = [frozenset(p["bbl"] for p in grp) for grp in merged.values()]

        self.assertIn(frozenset({"nj-1", "nj-2", "nj-3", "nj-4"}), bbl_sets)
        self.assertIn(frozenset({"nj-5"}), bbl_sets)
        self.assertIn(frozenset({"nj-6"}), bbl_sets)
        self.assertIn(frozenset({"nj-7"}), bbl_sets)
        self.assertIn(frozenset({"md-1", "md-2", "md-3"}), bbl_sets)
        self.assertIn(frozenset({"fl-1", "fl-2"}), bbl_sets)
        self.assertIn(frozenset({"pa-1", "pa-2"}), bbl_sets)

        # 3. Complex / trade-name strings must never be classified as human principals
        for non_person in (
            "THE CEDARS AT BASKING RIDGE",
            "EAGLE RIDGE AT W ORANGE",
            "DAVIS ENT",
            "RUTGERS BUILDERS T/A RUTGERSVILLAGE",
            "COVENTRY SQ",
            "WOODMERE AT EATONTOWN",
            "SAME TWO ABOVE",
            "SEE ATTACHED SCHEDULE",
        ):
            self.assertFalse(looks_like_person_owner(non_person), non_person)
        self.assertTrue(looks_like_person_owner("RAMAN KHOSLA"))
        self.assertTrue(looks_like_person_owner("WILLIAM CHRISTOPHER SMITH"))

    def test_va_unimproved_vacant_parcel_filter_and_ma_cama_registry(self):
        from ma.ma_municipal_config import MA_MUNICIPAL_DATA_SOURCES, ma_muni_status_key
        from states.ingest_va_parcels import _clean_va_site_address, _is_unimproved_vacant_va_parcel

        # 1. Caroline County $800 '0 UNASSIGNED' 'Vacant Land' (BLDG_VAL=0) paper lot is excluded
        self.assertIsNone(_clean_va_site_address("0 UNASSIGNED"))
        self.assertIsNone(_clean_va_site_address("UNASSIGNED"))
        self.assertEqual(_clean_va_site_address("4591 FORGE RD"), "4591 FORGE RD")

        self.assertTrue(
            _is_unimproved_vacant_va_parcel(
                has_imp_col=True,
                imp_val=0.0,
                tot_val=800.0,
                yr_built=None,
                units=None,
                land_use="Vacant Land",
                use_code="200V",
                real_site_addr=None,
            )
        )
        # 2. Genuine improved Virginia residential building (BLDG_VAL>0) is preserved
        self.assertFalse(
            _is_unimproved_vacant_va_parcel(
                has_imp_col=True,
                imp_val=193600.0,
                tot_val=229400.0,
                yr_built=1988,
                units=1,
                land_use="Single Family",
                use_code="100",
                real_site_addr="4591 FORGE RD",
            )
        )
        # 3. MA CAMA config covers Boston, Cambridge, Vision, and MapGeo portals cleanly
        self.assertGreaterEqual(len(MA_MUNICIPAL_DATA_SOURCES), 110)
        self.assertEqual(MA_MUNICIPAL_DATA_SOURCES["BOSTON"]["type"], "boston_assessing")
        self.assertEqual(MA_MUNICIPAL_DATA_SOURCES["CAMBRIDGE"]["type"], "cambridge_socrata")
        self.assertEqual(MA_MUNICIPAL_DATA_SOURCES["SOMERVILLE"]["type"], "vision_appraisal")
        self.assertEqual(MA_MUNICIPAL_DATA_SOURCES["NEWTON"]["type"], "mapgeo")
        self.assertEqual(ma_muni_status_key("Watertown"), "MA_MUNI_WATERTOWN")

    def test_false_positive_network_hardening_guards(self):
        from api.oc_linker import cluster_with_oc_and_series
        from ca.ca_county_config import ALL_58_CA_COUNTIES
        from fl.fl_county_config import FL_COUNTY_CONFIG
        from il.il_county_config import ALL_102_IL_COUNTIES
        from ny.ny_county_config import ALL_62_NY_COUNTIES

        # 1. Statewide county registries cover 100% of statutory counties
        self.assertEqual(len(FL_COUNTY_CONFIG), 67)
        self.assertEqual(len(ALL_62_NY_COUNTIES), 62)
        self.assertEqual(len(ALL_102_IL_COUNTIES), 102)
        self.assertEqual(len(ALL_58_CA_COUNTIES), 58)

        # 2. Abbreviated special districts & NNN/RA/truncated-unit hubs excluded
        self.assertTrue(is_excluded_third_party_entity("SCHOOL DIST # 65"))
        self.assertTrue(is_excluded_third_party_entity("FIRE DIST # 3"))
        for bad_hub in (
            "% KFC #X975058, PO BOX 35370, LOUISVILLE, KY 40232",
            "C/O WENDY'S PROPERTY TAX DEPT, 1 DAVE THOMAS BLVD, DUBLIN, OH 43017",
            "%CVS/HEALTH #7570-02, 1 CVS DRIVE MC 2320, WOONSOCKET, RI 02895",
            "405 2ND AVE S, MINNEAPOLIS, MN 55401",
            "5511 STAPLES MILL RD, RICHMOND, VA 23228",
            "1000 BRICKELL AVE STE 715, MIAMI, FL 33131",
            "508 CENTRAL PK AVE (UNIT SCARSDALE NY 10583",
        ):
            with self.subTest(bad_hub=bad_hub):
                self.assertTrue(is_excluded_third_party_address(bad_hub))

        # 3. Institutional 1-parcel stale address outlier ('13510 MANGO BAY DR') never sucks
        #    small buyer LLC ('BALCORP GROUP LLC') into a 20-home REIT SPV
        reit_props = [
            {
                "bbl": f"fl-prog-{i}",
                "address": f"{100 + i} PALM DR",
                "borough": "HILLSBOROUGH",
                "zip_code": "33579",
                "owner_name": "PROGRESS RESIDENTIAL BORROWER 12 LLC",
                "mailing_address": "PO BOX 31236, SALT LAKE CITY, UT 84131",
            }
            for i in range(18)
        ]
        reit_props.append({
            "bbl": "fl-prog-stale",
            "address": "12941 KINGS LAKE DR",
            "borough": "HILLSBOROUGH",
            "zip_code": "33579",
            "owner_name": "PROGRESS RESIDENTIAL BORROWER 12 LLC",
            "mailing_address": "13510 MANGO BAY DR, RIVERVIEW, FL 33579",
        })
        reit_props.append({
            "bbl": "fl-balcorp-1",
            "address": "2402 N ARMENIA AVE",
            "borough": "HILLSBOROUGH",
            "zip_code": "33607",
            "owner_name": "BALCORP GROUP LLC",
            "mailing_address": "13510 MANGO BAY DR, RIVERVIEW, FL 33579",
        })
        groups = run_union_find_clustering(reit_props, "fl")
        cluster_sets = [frozenset(p["bbl"] for p in grp) for grp in groups.values()]
        self.assertIn(frozenset({"fl-balcorp-1"}), cluster_sets)
        self.assertEqual(len(groups), 2)

        # 4. Natural-person homonyms across >=4 unrelated home addresses stay distinct,
        #    AND >=5 owner-occupants in the same condo building never merge into 1 landlord
        ny_props = [
            {"bbl": f"ny-wei-{i}", "address": f"{10 + i} MAIN ST", "borough": f"COUNTY_{i}", "zip_code": f"1000{i}",
             "owner_name": "LI, WEI", "mailing_address": f"{500 + i} OAK RD, TOWN{i}, NY 1000{i}"}
            for i in range(5)
        ]
        ny_props.append({
            "bbl": "ny-wei-pair", "address": "99 ELM ST", "borough": "COUNTY_0", "zip_code": "10000",
            "owner_name": "LI, WEI", "mailing_address": "500 OAK RD, TOWN0, NY 10000",
        })
        for idx, u_owner in enumerate(("ANTIARIS MARGARET", "APEL ANATOLY", "BRAGIN BELLA", "CHOI NOAH", "FRIED IRENE")):
            ny_props.append({
                "bbl": f"ny-condo-{idx}",
                "address": "508 CENTRAL PARK AVE",
                "borough": "WESTCHESTER",
                "zip_code": "10583",
                "owner_name": u_owner,
                "mailing_address": "508 CENTRAL PARK AVE, SCARSDALE, NY 10583",
            })
        ny_groups = run_union_find_clustering(ny_props, "ny")
        ny_sets = [frozenset(p["bbl"] for p in grp) for grp in ny_groups.values()]
        self.assertIn(frozenset({"ny-wei-0", "ny-wei-pair"}), ny_sets)
        self.assertIn(frozenset({"ny-wei-1"}), ny_sets)
        self.assertIn(frozenset({"ny-condo-0"}), ny_sets)
        self.assertIn(frozenset({"ny-condo-4"}), ny_sets)

        # 5. CPA/incorporator human spanning >12 unrelated 2-token LLC families is suppressed
        cpa_props = [
            {
                "bbl": f"fl-cpa-{i}",
                "owner_name": f"RANDOMALPHA{i} INVESTMENTS LLC",
                "oc_jurisdiction_code": "us_fl",
                "oc_company_number": f"L{i}",
            }
            for i in range(15)
        ]
        cpa_init = {p["bbl"]: [p] for p in cpa_props}
        cpa_oc = {
            ("us_fl", f"L{i}"): {
                "humans": ["JOHN CPAAGENT"],
                "holding_entities": [],
                "reached_companies": [f"us_fl/L{i}"],
                "hq_addresses": [],
                "raw_hq_addresses": [],
            }
            for i in range(15)
        }
        cpa_merged, cpa_m_cnt = cluster_with_oc_and_series(cpa_props, cpa_init, cpa_oc)
        self.assertEqual(cpa_m_cnt, 0)
        self.assertEqual(len(cpa_merged), 15)

    def test_ct_parity_quota_free_large_networks_and_trustee_samia_bridges(self):
        from api.oc_linker import (
            OCCandidate,
            cluster_with_oc_and_series,
            demangle_assessor_owner,
            score_oc_candidates,
        )
        from api.shared_utils import strip_benign_care_of_prefix

        # 1. MassGIS 'C/O US,' / 'C/O USA,' country-column shift is stripped cleanly, never excluded as US Gov
        samia_hq_raw = "C/O US, 60 LEO BIRMINGHAM PKWY, BRIGHTON, MA, 02135"
        self.assertEqual(
            strip_benign_care_of_prefix(samia_hq_raw),
            "60 LEO BIRMINGHAM PKWY, BRIGHTON, MA, 02135",
        )
        self.assertFalse(is_excluded_third_party_address(samia_hq_raw))

        # 2. 120 street-named LLCs at one cohesive landlord HQ ('39 BRIGHTON AVE ALLSTON' + 'BOSTON' 02134)
        #    unify into ONE single 180-building network without hitting any 12 / 30 / 75 quota!
        hamilton_props = []
        for i in range(120):
            city_alias = "ALLSTON" if (i % 2 == 0) else "BOSTON"
            hamilton_props.append({
                "bbl": f"ma-ham-{i}",
                "address": f"{10 + i} COMMONWEALTH AVE",
                "borough": "BOSTON",
                "zip_code": "02134",
                "owner_name": f"{10 + i} COMMONWEALTH STREET LLC",
                "mailing_address": f"39 BRIGHTON AVE, {city_alias}, MA, 02134",
            })
            if i < 60:
                hamilton_props.append({
                    "bbl": f"ma-ham-extra-{i}",
                    "address": f"{500 + i} BEACON ST",
                    "borough": "BROOKLINE",
                    "zip_code": "02446",
                    "owner_name": f"{10 + i} COMMONWEALTH STREET LLC",
                    "mailing_address": f"39 BRIGHTON AV, {city_alias}, MA, 02134",
                })
        ham_groups = run_union_find_clustering(hamilton_props, "ma")
        self.assertEqual(len(ham_groups), 1)
        self.assertEqual(len(next(iter(ham_groups.values()))), 180)

        # 3. Multi-suite office tower (>=3 distinct #suites) keeps suites separate without collapsing building
        tower_props = [
            {"bbl": "t-1", "address": "1 A ST", "borough": "BOSTON", "owner_name": "ALPHA TOWER LLC", "mailing_address": "100 HIGH ST STE 101, BOSTON, MA 02110"},
            {"bbl": "t-2", "address": "2 A ST", "borough": "BOSTON", "owner_name": "BETA TOWER LLC", "mailing_address": "100 HIGH ST #101, BOSTON, MA 02110"},
            {"bbl": "t-3", "address": "3 A ST", "borough": "BOSTON", "owner_name": "GAMMA TOWER LLC", "mailing_address": "100 HIGH ST STE 202, BOSTON, MA 02110"},
            {"bbl": "t-4", "address": "4 A ST", "borough": "BOSTON", "owner_name": "DELTA TOWER LLC", "mailing_address": "100 HIGH ST STE 303, BOSTON, MA 02110"},
        ]
        tower_groups = run_union_find_clustering(tower_props, "ma")
        tower_sets = [frozenset(p["bbl"] for p in grp) for grp in tower_groups.values()]
        self.assertIn(frozenset({"t-1", "t-2"}), tower_sets)
        self.assertIn(frozenset({"t-3"}), tower_sets)
        self.assertIn(frozenset({"t-4"}), tower_sets)

        # 4. Divergent external-HQ lockbox/manager address (>=5 families with >=4 external primary HQs) is quarantined
        div_props = []
        for f_idx in range(6):
            fam_nm = f"INDEPOWNER{f_idx} HOLDINGS LLC"
            for k in range(3):
                div_props.append({
                    "bbl": f"div-home-{f_idx}-{k}",
                    "address": f"{20 + k} ELM ST",
                    "borough": "WORCESTER",
                    "owner_name": fam_nm,
                    "mailing_address": f"{800 + f_idx * 10} MAIN ST, TOWN{f_idx}, MA 0160{f_idx}",
                })
            div_props.append({
                "bbl": f"div-mgr-{f_idx}",
                "address": f"{90 + f_idx} OAK ST",
                "borough": "WORCESTER",
                "owner_name": fam_nm,
                "mailing_address": "777 SHARED MANAGER WAY, WORCESTER, MA 01609",
            })
        div_groups = run_union_find_clustering(div_props, "ma")
        self.assertEqual(len(div_groups), 6)

        # 5. Tier 2b canonical stem match ('CHESTNUT HILL REALTY' -> 'CHESTNUT HILL REALTY CORPORATION'),
        #    Samia middle-name/repeated-token collapse, and direct trustee parcel ('ZUKER EDWARD E TS') bridge!
        rec_chr = demangle_assessor_owner("CHESTNUT HILL REALTY", mailing_address="300 INDEPENDENCE DR, CHESTNUT HILL, MA 02467")
        cand_chr = OCCandidate(
            jurisdiction_code="us_ma",
            company_number="042000001",
            name="CHESTNUT HILL REALTY CORP.",
            name_norm="CHESTNUT HILL REALTY CORPORATION",
            is_active=True,
            registered_address_coarse="300 INDEPENDENCE DRIVE CHESTNUT HILL MA",
        )
        m_chr = score_oc_candidates(
            rec_chr,
            parcel_state="MA",
            parcel_mailing_coarse="300 INDEPENDENCE DRIVE CHESTNUT HILL MA",
            candidates=[cand_chr],
        )
        self.assertIsNotNone(m_chr)
        self.assertEqual(m_chr.company_number, "042000001")

        oc_props = [
            {"bbl": "s-1", "owner_name": "SAMIA ALPHA LLC", "borough": "BOSTON", "oc_jurisdiction_code": "us_ma", "oc_company_number": "S1"},
            {"bbl": "s-2", "owner_name": "MARLBORO GARDENS LP", "borough": "MARLBOROUGH", "oc_jurisdiction_code": "us_ma", "oc_company_number": "S2"},
            {"bbl": "s-3", "owner_name": "ARLINGTON VIEW LLC", "borough": "ARLINGTON", "oc_jurisdiction_code": "us_ma", "oc_company_number": "S3"},
            {"bbl": "s-4", "owner_name": "SAMIA LEONARD J", "borough": "ANDOVER", "mailing_address": samia_hq_raw},
            {"bbl": "z-1", "owner_name": "CHR VILLAGE LLC", "borough": "BROOKLINE", "oc_jurisdiction_code": "us_ma", "oc_company_number": "Z1"},
            {"bbl": "z-2", "owner_name": "ZUKER EDWARD E TS", "borough": "BROOKLINE", "mailing_address": "300 INDEPENDENCE DR, CHESTNUT HILL, MA 02467"},
        ]
        oc_init = {p["bbl"]: [p] for p in oc_props}
        oc_res = {
            ("us_ma", "S1"): {"humans": ["ALEXANDRA SAMIA SOLIA", "LEONARD SAMIA"], "holding_entities": [], "reached_companies": ["us_ma/S1"], "hq_addresses": ["60 LEO BIRMINGHAM PARKWAY BRIGHTON MA"], "raw_hq_addresses": ["60 LEO BIRMINGHAM PKWY, BRIGHTON, MA 02135"]},
            ("us_ma", "S2"): {"humans": ["ALEXANDRA ANN SAMIA SOLIA"], "holding_entities": [], "reached_companies": ["us_ma/S2"], "hq_addresses": [], "raw_hq_addresses": []},
            ("us_ma", "S3"): {"humans": ["ALEXANDRA SAMIA SOLIA SOLIA"], "holding_entities": [], "reached_companies": ["us_ma/S3"], "hq_addresses": [], "raw_hq_addresses": []},
            ("us_ma", "Z1"): {"humans": ["EDWARD ZUKER"], "holding_entities": [], "reached_companies": ["us_ma/Z1"], "hq_addresses": ["300 INDEPENDENCE DRIVE CHESTNUT HILL MA"], "raw_hq_addresses": ["300 INDEPENDENCE DR, CHESTNUT HILL, MA 02467"]},
        }
        oc_merged, _ = cluster_with_oc_and_series(oc_props, oc_init, oc_res)
        oc_sets = [frozenset(p["bbl"] for p in grp) for grp in oc_merged.values()]
        self.assertIn(frozenset({"s-1", "s-2", "s-3", "s-4"}), oc_sets)
        self.assertIn(frozenset({"z-1", "z-2"}), oc_sets)

        # 6. Statewide common-name homonym disambiguation: 'RONALD BROWN' with 10 Hamilton LLCs at
        #    39 Brighton Ave must unify 100% WITHOUT absorbing unrelated 'RONALD BROWN's at
        #    different HQs ('74 Clarendon St', '99 Elm St', '200 Oak St')!
        hom_props = [
            {"bbl": f"hb-{i}", "owner_name": f"{10 + i} BRIGHTON STREET LLC", "borough": "BOSTON",
             "mailing_address": "39 BRIGHTON AVE, ALLSTON, MA, 02134",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": f"HB{i}"}
            for i in range(10)
        ] + [
            {"bbl": "rb-other-1", "owner_name": "CLARENDON TOWER LLC", "borough": "BOSTON",
             "mailing_address": "74 CLARENDON ST, BOSTON, MA, 02116",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": "RBO1"},
            {"bbl": "rb-other-2", "owner_name": "WORCESTER MILL LLC", "borough": "WORCESTER",
             "mailing_address": "99 ELM ST, WORCESTER, MA, 01609",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": "RBO2"},
            {"bbl": "rb-other-3", "owner_name": "SPRINGFIELD PLAZA LLC", "borough": "SPRINGFIELD",
             "mailing_address": "200 OAK ST, SPRINGFIELD, MA, 01103",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": "RBO3"},
        ]
        hom_init = {p["bbl"]: [p] for p in hom_props}
        hom_oc = {
            ("us_ma", f"HB{i}"): {
                "humans": ["RONALD BROWN", "JAMESON BROWN"],
                "holding_entities": [],
                "reached_companies": [f"us_ma/HB{i}"],
                "hq_addresses": ["39 BRIGHTON AVENUE ALLSTON MA"],
                "raw_hq_addresses": ["39 BRIGHTON AVE, ALLSTON, MA 02134"],
            }
            for i in range(10)
        }
        hom_oc[("us_ma", "RBO1")] = {"humans": ["RONALD BROWN"], "holding_entities": [], "reached_companies": ["us_ma/RBO1"], "hq_addresses": ["74 CLARENDON STREET BOSTON MA"], "raw_hq_addresses": []}
        hom_oc[("us_ma", "RBO2")] = {"humans": ["RONALD BROWN"], "holding_entities": [], "reached_companies": ["us_ma/RBO2"], "hq_addresses": ["99 ELM STREET WORCESTER MA"], "raw_hq_addresses": []}
        hom_oc[("us_ma", "RBO3")] = {"humans": ["RONALD BROWN"], "holding_entities": [], "reached_companies": ["us_ma/RBO3"], "hq_addresses": ["200 OAK STREET SPRINGFIELD MA"], "raw_hq_addresses": []}
        hom_merged, _ = cluster_with_oc_and_series(hom_props, hom_init, hom_oc)
        hom_sets = [frozenset(p["bbl"] for p in grp) for grp in hom_merged.values()]
        self.assertIn(frozenset(f"hb-{i}" for i in range(10)), hom_sets)
        self.assertIn(frozenset({"rb-other-1"}), hom_sets)
        self.assertIn(frozenset({"rb-other-2"}), hom_sets)
        self.assertIn(frozenset({"rb-other-3"}), hom_sets)

    def test_sparse_hq_uncommon_principal_and_parking_exclusion(self):
        from api.oc_linker import cluster_with_oc_and_series

        # 1. Commercial parking garage operators are excluded from residential landlord networks
        self.assertTrue(is_excluded_third_party_entity("Propark America West LLC"))
        self.assertTrue(is_excluded_third_party_entity("LAZ PARKING CALIFORNIA LLC"))
        self.assertFalse(is_excluded_third_party_entity("Portola Heights LLC"))

        # 2. Sparse-HQ uncommon principal (e.g. CA redacted/blank mailing addresses + RA LLCs):
        #    12 separate LLCs owned by uncommon principal 'NEVEO MOSSER' with 0 mailing HQs
        #    still bridge cleanly into 1 network (not artificially stopped at 8 entities!).
        ca_props = [
            {
                "bbl": f"ca-{i}",
                "owner_name": f"MOSSER BUILDING {i} LLC",
                "borough": "SAN FRANCISCO",
                "mailing_address": "",
                "oc_jurisdiction_code": "us_ca",
                "oc_company_number": f"CA{i}",
            }
            for i in range(12)
        ]
        ca_init = {p["bbl"]: [p] for p in ca_props}
        ca_oc = {
            ("us_ca", f"CA{i}"): {
                "humans": ["NEVEO MOSSER"],
                "holding_entities": [],
                "reached_companies": [f"us_ca/CA{i}"],
                "hq_addresses": [],
                "raw_hq_addresses": [],
            }
            for i in range(12)
        }
        ca_merged, _ = cluster_with_oc_and_series(ca_props, ca_init, ca_oc)
        self.assertEqual(len(ca_merged), 1)
        self.assertEqual(len(next(iter(ca_merged.values()))), 12)

    def test_depth4_bfs_salience_blocks_peripheral_attorney_bridge_between_independent_empires(self):
        from api.oc_linker import cluster_with_oc_and_series

        # Empire A (Hamilton Co, 5 LLCs at 39 Brighton Ave, Jameson Brown) + 1 affiliate LLC at old addr
        # Empire B (First Realty, 5 LLCs at 151 Tremont St, William Kargman)
        # Peripheral closing attorney 'SALLY E MICHAEL' appears on 1 LLC of Empire A and 1 LLC of Empire B.
        # Depth-4 BFS MUST merge Jameson Brown's affiliate into Empire A while keeping Empire A & B SEPARATE!
        props = [
            {"bbl": f"a-{i}", "owner_name": f"BRIGHTON {i} LLC", "borough": "BOSTON",
             "mailing_address": "39 BRIGHTON AVE, ALLSTON, MA 02134",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": f"MA_A{i}"}
            for i in range(5)
        ] + [
            {"bbl": "a-aff", "owner_name": "HARBOR ANNEX LLC", "borough": "CAMBRIDGE",
             "mailing_address": "88 HARBOR RD, CAMBRIDGE, MA 02138",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": "MA_AAFF"},
        ] + [
            {"bbl": f"b-{i}", "owner_name": f"TREMONT {i} LLC", "borough": "BOSTON",
             "mailing_address": "151 TREMONT ST, BOSTON, MA 02111",
             "oc_jurisdiction_code": "us_ma", "oc_company_number": f"MA_B{i}"}
            for i in range(5)
        ]
        init_groups = {
            "root_A": props[:5],
            "root_AFF": [props[5]],
            "root_B": props[6:],
        }
        oc_res = {}
        for i in range(5):
            humans = ["JAMESON BROWN", "RONALD BROWN"] + (["SALLY E MICHAEL"] if i == 0 else [])
            oc_res[("us_ma", f"MA_A{i}")] = {
                "humans": humans, "holding_entities": [], "reached_companies": [f"us_ma/MA_A{i}"],
                "hq_addresses": ["39 BRIGHTON AVENUE ALLSTON MA"], "raw_hq_addresses": [],
            }
        oc_res[("us_ma", "MA_AAFF")] = {
            "humans": ["JAMESON BROWN"], "holding_entities": [], "reached_companies": ["us_ma/MA_AAFF"],
            "hq_addresses": ["88 HARBOR ROAD CAMBRIDGE MA"], "raw_hq_addresses": [],
        }
        for i in range(5):
            humans = ["WILLIAM KARGMAN", "ROBERT KARGMAN"] + (["SALLY E MICHAEL"] if i == 0 else [])
            oc_res[("us_ma", f"MA_B{i}")] = {
                "humans": humans, "holding_entities": [], "reached_companies": [f"us_ma/MA_B{i}"],
                "hq_addresses": ["151 TREMONT STREET BOSTON MA"], "raw_hq_addresses": [],
            }
        merged, _ = cluster_with_oc_and_series(props, init_groups, oc_res)
        clusters = [frozenset(p["bbl"] for p in grp) for grp in merged.values()]
        expected_a = frozenset([f"a-{i}" for i in range(5)] + ["a-aff"])
        expected_b = frozenset([f"b-{i}" for i in range(5)])
        self.assertIn(expected_a, clusters)
        self.assertIn(expected_b, clusters)
        self.assertEqual(len(clusters), 2)

    def test_legal_aid_nonprofit_clt_hdfc_condo_coop_and_cmra_exclusions(self):
        # Universal non-landlord exclusions uncovered in 25-jurisdiction legal-aid audit
        for non_landlord in (
            "ROLLING MEADOWS COOP",
            "BELT'S LANDING, A CONDOMINIUM, INCORPORATED",
            "A Country Place Condominium",
            "THE REED CONDO",
            "SOCIETY HILL AT BERNARDS 1 CONDO",
            "Winfield Mutual Housing Corportation",
            "CONSOLIDATED RAIL CORPORATION ATTN: TAX DEPARTMENT",
            "WASTE MANAGEMENT OF MASSACHUSETTS, INC.",
            "AIRTOUCH CELLULAR INC.",
            "MHANY 2015 HOUSING DEVELOPMENT FUND",
            "ZEVRONE REALTY HOUSING DEVELOPMENT FUND",
            "CHAMPLAIN HOUSING TRUST",
            "WINDHAM & WINDSOR HOUSING TRUST INC",
            "HOUSING FOUNDATION INC",
            "TENANTS DEVELOPMENT CORPORATION",
            "WEST BANK COMMUNITY DEV CORP",
            "BOSTON NATURAL AREAS FUND",
            "NEIGHBORSPACE",
            "CHRISTIAN RELIEF SERVICES OF VIRGINIA INC",
            "ULBRICH BOYS & GIRLS CLUB, INC.",
            "WILLINGTON FISH AND GAME CLUB INC., THE",
            "CONGREGATION BH, INC.",
            "STEWART'S SHOPS CORP",
            "7-ELEVEN INC",
            "WYNDHAM VACATION RESORTS INC",
            "SECU*RE INC",
            "ANDERSON STEVENSON WILKE FUNERAL & CREMATION SERVICES INC",
            "Condominium Building - See Individual Unit Records for Additional Information",
            "COUNCIL OF UNIT OWNERS OF THE 1209 NORTH CHARLES CONDOMINIUM, INC",
        ):
            with self.subTest(non_landlord=non_landlord):
                self.assertTrue(is_excluded_third_party_entity(non_landlord))

        # Genuine private landlords that share substrings must NEVER be excluded
        for private_landlord in (
            "1628 CHURCH ST, LLC",
            "WABASH CONDOMINIUM LLC",
            "BUSHNELL CONDOS, LLC",
            "CJ COOP, LLC",
            "GOODWILL PARTNERS LLC",
            "SFR 2 DE LLC",
            "WINNCOMPANIES",
            "NEW HAVEN REALTY TRUST",
        ):
            with self.subTest(private_landlord=private_landlord):
                self.assertFalse(is_excluded_third_party_entity(private_landlord))

        # Leading-zero streetless floor, bare cityless PO Boxes, and city-scoped CMRA/RA hubs
        for bad_addr in (
            "03RD FL, PHILADELPHIA PA, 19143",
            "003RD FLOOR, BOSTON MA, 02108",
            "PO BOX 333",
            "PO BOX 520, MT",
            "1602 FRANKFORD AVE #3757, PHILADELPHIA PA, 19125",
            "1001 S MAIN ST STE 7116, KALISPELL, MT 59901",
            "3948 3RD ST S #219, JACKSONVILLE BEACH, FL 32250",
            "1716 CAPITOL AVE STE 100, CHEYENNE, WY 82001",
        ):
            with self.subTest(bad_addr=bad_addr):
                self.assertTrue(is_excluded_third_party_address(bad_addr))

        for good_addr in (
            "PO BOX 333, BILLINGS, MT 59103",
            "1001 S MAIN ST, HARTFORD, CT 06103",
            "25 S 19TH ST 2ND FL, PHILADELPHIA PA, 19103",
        ):
            with self.subTest(good_addr=good_addr):
                self.assertFalse(is_excluded_third_party_address(good_addr))

    def test_structural_four_digit_cmra_mailbox_separation_in_clustering(self):
        from api.populate_cities import run_union_find_clustering

        # Two unrelated LLCs sharing a street address with distinct 4-to-5-digit mailbox numbers
        # must NEVER collapse onto the base street address (structural CMRA/virtual-office guard).
        properties = [
            {
                "bbl": "CMRA-1",
                "owner_name": "ALPHA HOLDINGS LLC",
                "mailing_address": "999 MARKET ST #3757, ANYTOWN, PA 19125",
                "units_res": 4,
                "borough": "ANYTOWN",
            },
            {
                "bbl": "CMRA-2",
                "owner_name": "OMEGA PROPERTIES LLC",
                "mailing_address": "999 MARKET ST UNIT #15204, ANYTOWN, PA 19125",
                "units_res": 6,
                "borough": "ANYTOWN",
            },
            {
                "bbl": "CMRA-3",
                "owner_name": "GAMMA REALTY LLC",
                "mailing_address": "999 MARKET ST, ANYTOWN, PA 19125",
                "units_res": 3,
                "borough": "ANYTOWN",
            },
        ]
        groups = run_union_find_clustering(properties, "philadelphia")
        self.assertEqual(len(groups), 3)


if __name__ == "__main__":
    unittest.main()
