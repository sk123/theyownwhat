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
            "WEST BANK COMMUNITY DEV CORP",
            "RIVERBANK REALTY LLC",
            "RED BANK APARTMENTS LLC",
            "1628 CHURCH ST, LLC",
            "3024 CHURCH AVENUE LLC",
            "1120 COLLEGE AVE HDFC",
            "55 UNIVERSITY PL ASSOCIATES LLC",
            "77 SCHOOL STREET CONDOMINIUM TRUST",
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


if __name__ == "__main__":
    unittest.main()
