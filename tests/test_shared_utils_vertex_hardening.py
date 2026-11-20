import unittest
import psycopg2
from api.shared_utils import (
    THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN,
    is_excluded_third_party_address,
    is_excluded_third_party_entity,
)


class TestSharedUtilsVertexHardening(unittest.TestCase):
    def test_non_landlord_utilities_gov_and_custodians(self):
        should_exclude = [
            "PPP FUTURE DEVELOPMENT, INC.",
            "GREENWAY ENERGY LLC",
            "AQUA NORTH CAROLINA INC",
            "HEATER UTILITIES INC",
            "FAIRWAYS UTILITIES",
            "DUKE ENERGY PROGRESS INC",
            "COLUMBIA GAS OF OHIO INC",
            "LUCAS COUNTY LAND REUTILIZATION CORP",
            "HAMILTON COUNTY LAND REUTILIZATION CORPORATION",
            "UNITED STATES GOVERNMENT",
            "UNITED STATES SECRETARY OF HOUSING",
            "****STATE****",
            "STATE",
            "HISID",
            "WAKE COUNTY",
            "COMMON WEALTH TRUST SERVICES LLC",
            "LAND TRUST SERVICE CORPORATION",
            "MCDONALD'S REAL ESTATE CO",
            "BURGER KING COMPANY LLC",
            "ALDI INC CO RYAN TAX",
            "COCA COLA BEVERAGES FLORIDA LLC",
            "ARKANSAS MEMORIAL GARDENS INC",
            "SECU RE INC",
            "BOARD OF COMMISSIONERS OF THE METROPOLITAN PARK DISTRICT",
        ]
        for name in should_exclude:
            self.assertTrue(is_excluded_third_party_entity(name), f"Expected exclusion for: {name}")

    def test_real_landlords_not_falsely_excluded(self):
        must_keep = [
            "PROGRESS RESIDENTIAL BORROWER 1 LLC",
            "INVITATION HOMES LP",
            "TRICON SFR 2024-4 BORROWER LLC",
            "SHELBOURNE GLOBAL SOLUTIONS LLC",
            "GREENWAY APARTMENTS LLC",
            "STATE STREET PROPERTIES LLC",
            "WAKEFIELD APARTMENTS LLC",
            "AQUA VIEW RESIDENCES LLC",
            "COMMONWEALTH REALTY PARTNERS LLC",
            "BURGER PROPERTIES LLC",
            "ASYLUM STREET COOPERATIVE, LLC",
        ]
        for name in must_keep:
            self.assertFalse(is_excluded_third_party_entity(name), f"Must NOT exclude real landlord: {name}")

    def test_verified_cmra_tax_servicer_and_ra_hubs_blocked(self):
        blocked_addrs = [
            "% PROGRESS RESIDENTIAL C/O CANON LLC SALT LAKE CITY UT 84131",
            "1123 PARK VIEW DR COVINA CA 91724-3748",
            "433 E LAS COLINAS BLVD, IRVING, TX, 75039",
            "1 WASHINGTON MALL STE 500, BOSTON, MA, 02108",
            "103 FOULK RD # 900 WILMINGTON DE 19803",
            "137 MITCHELLS CHANCE RD, SUITE 300, EDGEWATER, MD, 21037",
            "10006 CROSS CREEK BLVD STE 211 TAMPA 33647",
            "10031 PINES BLVD #228 PEMBROKE PINES FL 33024",
            "1101 SUNSET RD UNIT 680913 CHARLOTTE NC 28216",
            "1879 WHITEHAVEN ROAD, SUITE 1012, GRAND ISLAND, NY, 14072",
            "1187 WHITNEY MESA DR # 3380 HENDERSON NV 89014",
            "1800 JFK BLVD STE 300 PMB 2187, PHILADELPHIA PA, 19103",
            "1000 RIVERSIDE AVE STE 600 JACKSONVILLE FL 32204",
            "1035 OSAGE ST DENVER CO 80204",
            "1000 LOWES BLVD MOORESVILLE NC 28117",
            "1000 DARDEN CENTER DR, ORLANDO, FL, 32837",
            "C/O RYAN TAX, 15 W 6TH ST, TULSA, OK 74119",
        ]
        for addr in blocked_addrs:
            self.assertTrue(is_excluded_third_party_address(addr), f"Expected hub exclusion for: {addr}")

    def test_postgres_are_regex_compiles_cleanly(self):
        conn = psycopg2.connect("postgresql://user:password@127.0.0.1:5432/ctdata")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT %s ~* %s, %s ~* %s",
                    (
                        "PPP FUTURE DEVELOPMENT, INC.",
                        THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN,
                        "PROGRESS RESIDENTIAL BORROWER LLC",
                        THIRD_PARTY_INSTITUTIONAL_SQL_PATTERN,
                    ),
                )
                hit, miss = cur.fetchone()
                self.assertTrue(hit)
                self.assertFalse(miss)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
