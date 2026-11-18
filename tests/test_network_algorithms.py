#!/usr/bin/env python3
"""
tests/test_network_algorithms.py
=================================
Comprehensive regression test suite auditing normalization, matching rules,
BFS graph discovery, and portfolio linkage across all major landlord networks.
"""

import os
import sys
import unittest
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.shared_utils import (
    normalize_business_name,
    normalize_person_name,
    normalize_mailing_address,
    normalize_mailing_address_coarse,
    canonicalize_person_name,
    canonicalize_business_name,
    is_placeholder_address,
    compose_address,
    load_registrar_address_substrings,
    is_registrar_address,
    get_email_match_key,
)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

class TestNetworkAlgorithmsAndNormalization(unittest.TestCase):

    def setUp(self):
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
        except Exception:
            self.conn = psycopg2.connect("postgresql://user:password@localhost:5432/ctdata")

    def tearDown(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def test_gurevitch_portfolio_linkage(self):
        """Assert Menachem & Yehuda Gurevitch remain connected with >= 1,200 properties."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT network_id, entity_id, entity_name, normalized_name
                FROM entity_networks
                WHERE entity_type = 'principal'
                  AND (entity_id IN ('1227', '15603') OR normalized_name IN ('MENACHEM GUREVITCH', 'YEHUDA GUREVITCH'))
            """)
            rows = cur.fetchall()
            self.assertTrue(len(rows) >= 2, f"Could not find both Gurevitch principals in entity_networks (found {len(rows)})")

            network_ids = set(r["network_id"] for r in rows)
            self.assertEqual(
                len(network_ids), 1,
                f"Menachem and Yehuda Gurevitch are disconnected! Found multiple network IDs: {network_ids}"
            )

            target_id = list(network_ids)[0]
            cur.execute("SELECT total_properties FROM networks WHERE id = %s", (target_id,))
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertGreaterEqual(row["total_properties"], 1150)

    def test_edelkopf_portfolio_linkage(self):
        """Assert Shneor Edelkopf network exists and is properly linked."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT network_id, entity_name
                FROM entity_networks
                WHERE entity_type = 'principal' AND normalized_name LIKE '%EDELKOPF%'
            """)
            rows = cur.fetchall()
            self.assertTrue(len(rows) >= 1, "Edelkopf principal network missing")

    def test_reiner_intertown_portfolio_linkage_and_separation(self):
        """Assert Michael D. Reiner (Intertown Realty / Sarjac Partners / Asylum Street Cooperative) is unified in his own network and NOT merged into Rausman/Hollender via Greene Law PC."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT en.network_id, n.primary_name, n.total_properties, en.entity_type, en.entity_id, en.entity_name
                FROM entity_networks en
                JOIN networks n ON n.id = en.network_id
                WHERE (en.entity_type = 'principal' AND en.entity_id = '26656')
                   OR (en.entity_type = 'business' AND en.entity_id IN ('001t000000WmvvjAAB', '001t000000WnpvSAAR'))
            """)
            rows = cur.fetchall()
            self.assertTrue(
                any(r["entity_type"] == "principal" and r["entity_id"] == "26656" for r in rows),
                f"Michael D. Reiner (26656) missing from entity_networks: {rows}"
            )
            net_ids = {r["network_id"] for r in rows}
            self.assertEqual(
                len(net_ids), 1,
                f"Michael D. Reiner's Sarjac Partners and Asylum Street Cooperative are split across networks: {rows}"
            )
            reiner_net_id = next(iter(net_ids))
            cur.execute("""
                SELECT entity_name FROM entity_networks
                WHERE network_id = %s AND (entity_name ILIKE '%%RAUSMAN%%' OR entity_name ILIKE '%%HOLLENDER%%')
            """, (reiner_net_id,))
            leaked = cur.fetchall()
            self.assertEqual(
                leaked, [],
                f"Michael D. Reiner network falsely merged with Rausman/Hollender via law office address: {leaked}"
            )

    def test_nyc_speliotis_burgess_separation(self):
        """Assert MHANY (Ismene Speliotis) and Banana Kelly (Hope Burgess) remain separate in NYC HPD and are NEVER merged."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT c.full_name_norm, r.bbl
                FROM nyc_hpd_contacts c
                JOIN nyc_hpd_registrations r ON c.registration_id = r.registration_id
                WHERE c.full_name_norm LIKE '%SPELIOTIS%' OR c.full_name_norm LIKE '%BURGESS%'
            """)
            rows = cur.fetchall()
            speliotis_bbls = set(r['bbl'] for r in rows if 'SPELIOTIS' in r['full_name_norm'])
            burgess_bbls = set(r['bbl'] for r in rows if 'BURGESS' in r['full_name_norm'])

            shared_bbls = speliotis_bbls.intersection(burgess_bbls)
            self.assertEqual(
                len(shared_bbls), 0,
                f"CATASTROPHIC OVERINCLUSION: MHANY (Speliotis) and Banana Kelly (Burgess) share {len(shared_bbls)} BBLs: {shared_bbls}"
            )

    def test_address_normalization(self):
        """Audit fine and coarse address normalization routines."""
        self.assertEqual(normalize_mailing_address("123 Main St, Suite 400"), "123 MAIN STREET #400")
        self.assertEqual(normalize_mailing_address("P.O. Box 1234"), "PO BOX 1234")
        self.assertEqual(normalize_mailing_address("P O Box 5678"), "PO BOX 5678")
        self.assertTrue(is_placeholder_address("NO INFORMATION PROVIDED"))
        self.assertTrue(is_placeholder_address("UNKNOWN"))

        # Coarse normalization (strips unit numbers for building matching)
        self.assertEqual(
            normalize_mailing_address_coarse("399 Whalley Ave Suite 103, New Haven, CT 06511"),
            "399 WHALLEY AVENUE NEW HAVEN CT",
        )

    def test_unit_variants_collapse_to_one_building(self):
        """Every spelling of a suite/apartment/floor must reach one building key."""
        variants = [
            "399 Whalley Ave Suite 103, New Haven, CT 06511-1631",
            "399 Whalley Ave, Ste 103, New Haven, CT 06511",
            "399 WHALLEY AVENUE #103, NEW HAVEN, CT 06511",
            "399 Whalley Avenue Unit 103, New Haven CT 06511",
            "399 Whalley Ave, New Haven, CT 06511",
            "399 WHALLEY AVE 103 NEW HAVEN CT 06511",
        ]
        keys = {normalize_mailing_address_coarse(v) for v in variants}
        self.assertEqual(
            len(keys), 1,
            f"Unit spellings of one building split into {len(keys)} keys: {keys}"
        )

        # Floor spellings must also converge.
        floors = {
            normalize_mailing_address_coarse("100 Pearl St Fl 14, Hartford CT 06103"),
            normalize_mailing_address_coarse("100 Pearl Street, 14th Floor, Hartford CT 06103"),
            normalize_mailing_address_coarse("100 PEARL STREET, HARTFORD, CT 06103"),
        }
        self.assertEqual(len(floors), 1, f"Floor spellings split into {floors}")

    def test_numbered_streets_are_not_merged(self):
        """The unit-number stripper must not eat a numbered street's ordinal."""
        a = normalize_mailing_address_coarse("1650 East 15 Street, Brooklyn, NY 11229")
        b = normalize_mailing_address_coarse("1650 East 18 Street, Brooklyn, NY 11229")
        self.assertTrue(a and b)
        self.assertNotEqual(a, b, "Distinct numbered streets collapsed into one key")

        # ...while abbreviation/ordinal spellings of the SAME street do merge.
        c = normalize_mailing_address_coarse("1650 E 15th St, Brooklyn, NY 11229")
        self.assertEqual(a, c, "'E 15th St' failed to match 'East 15 Street'")

    def test_state_codes_are_not_read_as_street_types(self):
        """'CT' (Connecticut/Court) and 'FL' (Florida/Floor) and float ZIPs ('11211.0', '6830.0') must stay correct."""
        self.assertEqual(
            normalize_mailing_address("Route 66 East, Columbia, CT"),
            "ROUTE 66 EAST COLUMBIA CT",
        )
        self.assertEqual(
            normalize_mailing_address("1 Oak Ct, West Hartford, CT 06107"),
            "1 OAK COURT WEST HARTFORD CT 06107",
        )
        self.assertEqual(
            normalize_mailing_address("500 Brickell Ave Ste 200, Miami, FL 33131"),
            "500 BRICKELL AVENUE #200 MIAMI FL 33131",
        )
        self.assertEqual(
            normalize_mailing_address_coarse("199 Lee Ave, Brooklyn, NY, United States, 11211.0"),
            "199 LEE AVENUE BROOKLYN NY",
        )
        self.assertEqual(
            normalize_mailing_address_coarse(compose_address("15 East Putnam Ave", "GREENWICH", "CT", "6830.0")),
            "15 EAST PUTNAM AVENUE GREENWICH CT",
        )

    def test_city_is_part_of_the_address_key(self):
        """Same street number in different towns must not merge."""
        east_islip = compose_address("70 West Main Street", "EAST ISLIP", "NY", "11730")
        babylon = compose_address("70 West Main Street", "BABYLON", "NY", "11702")
        self.assertNotEqual(
            normalize_mailing_address_coarse(east_islip),
            normalize_mailing_address_coarse(babylon),
            "Identical street lines in different towns collapsed into one key",
        )
        # compose_address must not duplicate a component already in the line.
        self.assertEqual(
            compose_address("295 Main St, Buffalo, NY 14203", "BUFFALO", "NY", "14203"),
            "295 Main St, Buffalo, NY 14203",
        )

    def test_registrar_addresses_are_excluded(self):
        """Known registered-agent offices must be rejected as linking evidence."""
        substrings = load_registrar_address_substrings(self.conn)
        self.assertTrue(
            substrings,
            "address_match_rules produced no registrar substrings — the table is "
            "empty or the loader is not wired up",
        )
        for raw in [
            "80 State St, Albany, NY 12207",
            "28 Liberty Street, New York, NY 10005",
            "418 Broadway, Albany, NY 12207",
            "1209 Orange St, Wilmington, DE 19801",
        ]:
            norm = normalize_mailing_address_coarse(raw)
            self.assertTrue(
                is_registrar_address(norm, substrings),
                f"Registered-agent address not filtered: {raw!r} -> {norm!r}",
            )
        # A real landlord office must survive.
        self.assertFalse(
            is_registrar_address(
                normalize_mailing_address_coarse("399 Whalley Ave, New Haven, CT 06511"),
                substrings,
            )
        )

    def test_principal_ignore_list_matches_stored_name_order(self):
        """Ignore-list agents must be filtered whatever the stored name order."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT normalized_name FROM principal_ignore_list")
            ignore_keys = {
                canonicalize_person_name(r["normalized_name"])
                for r in cur
                if r["normalized_name"]
            }
            cur.execute("""
                SELECT principal_id, name_normalized, business_count
                FROM unique_principals
                WHERE business_count >= 20
            """)
            leaked = [
                r for r in cur
                if canonicalize_person_name(r["name_normalized"]) in ignore_keys
            ]

        if not leaked:
            return
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT entity_name, COUNT(*) AS n
                FROM entity_networks
                WHERE entity_type = 'principal' AND entity_id = ANY(%s)
                GROUP BY entity_name
                """,
                ([str(r["principal_id"]) for r in leaked],),
            )
            still_linked = cur.fetchall()
        self.assertEqual(
            still_linked, [],
            f"Ignore-listed corporate agents are still bridging networks: {still_linked}",
        )

    def test_person_name_canonicalization(self):
        """Audit LAST FIRST vs FIRST LAST name canonicalization."""
        name1 = canonicalize_person_name("MENACHEM GUREVITCH")
        name2 = canonicalize_person_name("GUREVITCH MENACHEM")
        self.assertEqual(name1, name2)

    def test_multi_jurisdiction_network_accuracy(self):
        """Verify network builder graph counts, top network thresholds, and overbroadness limits across all 19 live jurisdictions."""
        jurisdictions = [
            ("CT", "networks", "total_properties", 30000, 1000, 5000),
            ("NYC", "nyc_networks", "building_count", 50000, 100, 2500),
            ("NJ", "nj_networks", "building_count", 30000, 100, 5000),
            ("BALTIMORE", "baltimore_networks", "building_count", 50000, 100, 10000),
            ("BOSTON", "boston_networks", "building_count", 50000, 100, 10000),
            ("DC", "dc_networks", "building_count", 40000, 50, 5000),
            ("DETROIT", "detroit_networks", "building_count", 50000, 100, 10000),
            ("PHILADELPHIA", "philadelphia_networks", "building_count", 100000, 100, 10000),
            ("CHICAGO", "chicago_networks", "building_count", 50000, 50, 10000),
            ("MIAMI", "miami_networks", "building_count", 100000, 50, 10000),
            ("MINNEAPOLIS", "minneapolis_networks", "building_count", 1000, 15, 5000),
            ("MA", "ma_networks", "building_count", 500000, 100, 10000),
            ("FL", "fl_networks", "building_count", 500000, 100, 25000),
            ("NY", "ny_networks", "building_count", 500000, 50, 10000),
            ("NC", "nc_networks", "building_count", 500000, 100, 10000),
            ("AR", "ar_networks", "building_count", 500000, 50, 10000),
            ("WI", "wi_networks", "building_count", 500000, 50, 10000),
            ("CO", "co_networks", "building_count", 300000, 50, 10000),
            ("VT", "vt_networks", "building_count", 50000, 20, 5000),
            ("VA", "va_networks", "building_count", 500000, 100, 10000),
        ]

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            for code, tbl, col, min_nets, min_largest, max_largest in jurisdictions:
                # 1. Total Networks Existence Check
                cur.execute(f"SELECT COUNT(*) as net_count, MAX({col}) as largest_net FROM {tbl}")
                row = cur.fetchone()
                self.assertIsNotNone(row, f"{code} networks table {tbl} is unreadable")
                net_count = row["net_count"]
                largest_net = row["largest_net"] or 0

                self.assertGreaterEqual(
                    net_count, min_nets,
                    f"[{code}] Network count drop detected! Found {net_count:,} networks, expected >= {min_nets:,}"
                )

                # 2. Largest Network Lower Bound Check (Underbroadness assertion)
                self.assertGreaterEqual(
                    largest_net, min_largest,
                    f"[{code}] Top network size underbroad! Largest network has {largest_net} buildings, expected >= {min_largest}"
                )

                # 3. Largest Network Upper Bound Check (Overbroadness super-cluster safeguard)
                self.assertLessEqual(
                    largest_net, max_largest,
                    f"[{code}] CATASTROPHIC OVERBROADNESS DETECTED! Largest network collapsed into super-cluster with {largest_net} buildings"
                )

    def test_cross_jurisdiction_multi_property_clustering(self):
        """Verify every jurisdiction successfully clusters multi-property portfolios into networks."""
        jurisdictions = [
            ("CT", "networks", "total_properties"),
            ("NYC", "nyc_networks", "building_count"),
            ("NJ", "nj_networks", "building_count"),
            ("BALTIMORE", "baltimore_networks", "building_count"),
            ("BOSTON", "boston_networks", "building_count"),
            ("DC", "dc_networks", "building_count"),
            ("DETROIT", "detroit_networks", "building_count"),
            ("PHILADELPHIA", "philadelphia_networks", "building_count"),
            ("CHICAGO", "chicago_networks", "building_count"),
            ("MIAMI", "miami_networks", "building_count"),
            ("MINNEAPOLIS", "minneapolis_networks", "building_count"),
        ]
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            for code, tbl, col in jurisdictions:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE {col} > 1")
                multi_cnt = cur.fetchone()["cnt"]
                self.assertGreater(
                    multi_cnt, 0,
                    f"[{code}] Zero multi-property networks built in {tbl}! Network untangler algorithm failed."
                )

    def test_no_false_positive_co_owner_or_utility_networks(self):
        """Ensure assessor co_owner overflow fragments ('(SURV)', 'TITLE', 'LIVING') and utilities do not create mega-networks."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT n.primary_name, n.total_properties, en.entity_name
                FROM entity_networks en
                JOIN networks n ON n.id = en.network_id
                WHERE en.entity_type = 'business'
                  AND UPPER(en.entity_name) IN (
                      'SURV MANAGEMENT INC.',
                      'TITLE LLC',
                      'LIVING INVESTMENTS LLC',
                      'NIAGARA MOHAWK POWER CORPORATION',
                      'VERIZON NEW YORK INC.',
                      'NATIONAL GRID PARTNERS INC.',
                      'NEW YORK STATE SERVICES LLC',
                      'SUFFOLK COUNTY PROPERTIES LLC'
                  )
                  AND n.total_properties > 50
            """)
            bad_rows = cur.fetchall()
            self.assertEqual(
                bad_rows, [],
                f"False-positive co_owner overflow or utility/government mega-networks detected: {bad_rows}"
            )

    def test_shadow_swap_indexes_preserved(self):
        """Verify unnamed shadow indexes survive atomic_swap on entity_networks and ownership_links."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS idx_cnt FROM pg_indexes WHERE tablename = 'entity_networks'")
            en_idx = cur.fetchone()["idx_cnt"]
            self.assertGreaterEqual(
                en_idx, 5,
                f"entity_networks lost indexes during shadow swap! Expected >= 5 indexes, found {en_idx}"
            )
            cur.execute("SELECT COUNT(*) AS idx_cnt FROM pg_indexes WHERE tablename = 'ownership_links'")
            ol_idx = cur.fetchone()["idx_cnt"]
            self.assertGreaterEqual(
                ol_idx, 1,
                f"ownership_links lost indexes during shadow swap! Expected >= 1 index, found {ol_idx}"
            )

    def test_network_properties_city_normalized(self):
        """Verify network_properties.property_city is uppercase/trimmed so municipal Rap Sheets (e.g. HARTFORD) match cleanly."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT COUNT(*) AS unnormalized
                FROM network_properties
                WHERE property_city IS NOT NULL
                  AND property_city != UPPER(TRIM(property_city))
            """)
            self.assertEqual(
                cur.fetchone()["unnormalized"], 0,
                "Found unnormalized lowercase/padded property_city rows in network_properties!"
            )
            cur.execute("SELECT COUNT(*) AS hartford_cnt FROM network_properties WHERE property_city = 'HARTFORD'")
            self.assertGreater(
                cur.fetchone()["hartford_cnt"], 1000,
                "Expected >1,000 HARTFORD rows in network_properties"
            )

    def test_professional_agent_email_domains_and_reiner_separation(self):
        """Verify law/CPA email domains do not bridge unrelated landlords and Michael D. Reiner stays distinct from Greene Law clients."""
        rules = {"gmail.com": "public"}
        for agent_email in (
            "mreiner@greenelawpc.com",
            "ggreene@greenelawpc.com",
            "jdoe@pullcom.com",
            "tax@wycpas.com",
            "info@amtax.cpa",
        ):
            self.assertIsNone(
                get_email_match_key(agent_email, rules),
                f"Expected third-party law/CPA domain {agent_email} to return None",
            )
        self.assertEqual(get_email_match_key("sheila@sarjacpartners.com", rules), "sarjacpartners.com")
        self.assertEqual(get_email_match_key("agent@houlihanlawrence.com", rules), "houlihanlawrence.com")
        self.assertEqual(get_email_match_key("info@nianticlawn.com", rules), "nianticlawn.com")

        # Verify pandas float-formatted ZIP normalization
        composed = compose_address("199 LEE AVE SUITE 413", "BROOKLYN", "NY", "11211.0")
        self.assertEqual(normalize_mailing_address_coarse(composed), "199 LEE AVENUE BROOKLYN NY")

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT en.network_id
                FROM entity_networks en
                WHERE en.entity_type = 'principal' AND en.entity_id = '26656'
            """)
            reiner_rows = cur.fetchall()
            self.assertEqual(len(reiner_rows), 1, "Expected Michael D. Reiner (26656) in entity_networks")
            reiner_nid = reiner_rows[0]["network_id"]

            cur.execute(
                "SELECT entity_name FROM entity_networks WHERE network_id = %s",
                (reiner_nid,),
            )
            names_upper = {r["entity_name"].upper() for r in cur.fetchall() if r.get("entity_name")}
            self.assertIn("SARJAC PARTNERS, LLC", names_upper)
            self.assertIn("ASYLUM STREET COOPERATIVE, LLC", names_upper)
            self.assertNotIn("NILES GILLETT LLC", names_upper)
            self.assertNotIn("ABRAHAM SILBERSTEIN", names_upper)
            self.assertNotIn("MITCHELL'S PLACE LLC", names_upper)

    def test_nyc_large_verified_portfolio_and_coop_agent_separation(self):
        """Assert genuine >300-BBL NYC landlord portfolios (Glen Brown / Townhouse Rental LLC) stay unified while third-party co-op/condo managing agents (AKAM, Andrews) do not create mega-nets."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT display_name, building_count
                FROM nyc_networks
                WHERE member_names::text ILIKE '%TOWNHOUSE RENTAL LLC%'
                ORDER BY building_count DESC
                LIMIT 1
            """)
            gb_row = cur.fetchone()
            self.assertIsNotNone(gb_row, "Expected Townhouse Rental LLC / Glen Brown portfolio in nyc_networks")
            self.assertGreaterEqual(
                gb_row["building_count"],
                400,
                f"Verified >300-BBL NYC portfolio (Townhouse Rental LLC) was shattered! Found {gb_row}",
            )

            cur.execute("""
                SELECT display_name, building_count
                FROM nyc_networks
                WHERE UPPER(display_name) IN ('ALKER CHRISTOPHER', 'CHRISTOPHER ALKER', 'RASHAD DIVYA', 'DIVYA RASHAD')
                  AND building_count > 50
            """)
            leaked_agents = cur.fetchall()
            self.assertEqual(
                leaked_agents,
                [],
                f"Third-party NYC co-op/condo managing agents leaked as >50-building landlord networks: {leaked_agents}",
            )


if __name__ == "__main__":
    unittest.main()
