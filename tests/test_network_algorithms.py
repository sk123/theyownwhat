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
    is_placeholder_address
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
            self.assertGreaterEqual(row["total_properties"], 1200)

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

    def test_address_normalization(self):
        """Audit fine and coarse address normalization routines."""
        self.assertEqual(normalize_mailing_address("123 Main St, Suite 400"), "123 MAIN STREET #400")
        self.assertEqual(normalize_mailing_address("P.O. Box 1234"), "PO BOX 1234")
        self.assertEqual(normalize_mailing_address("P O Box 5678"), "PO BOX 5678")
        self.assertTrue(is_placeholder_address("NO INFORMATION PROVIDED"))
        self.assertTrue(is_placeholder_address("UNKNOWN"))

        # Coarse normalization (strips unit numbers for building matching)
        self.assertEqual(normalize_mailing_address_coarse("399 Whalley Ave Suite 103, New Haven, CT 06511"), "399 WHALLEY AVENUE NEW HAVEN")

    def test_person_name_canonicalization(self):
        """Audit LAST FIRST vs FIRST LAST name canonicalization."""
        name1 = canonicalize_person_name("MENACHEM GUREVITCH")
        name2 = canonicalize_person_name("GUREVITCH MENACHEM")
        self.assertEqual(name1, name2)

    def test_multi_jurisdiction_network_accuracy(self):
        """Verify network builder graph counts, top network thresholds, and overbroadness limits across all jurisdictions."""
        jurisdictions = [
            ("CT", "networks", "total_properties", 50000, 1000, 1000000),
            ("NYC", "nyc_networks", "building_count", 50000, 100, 500000),
            ("NJ", "nj_networks", "building_count", 30000, 100, 200000),
            ("BALTIMORE", "baltimore_networks", "building_count", 50000, 100, 300000),
            ("BOSTON", "boston_networks", "building_count", 50000, 100, 200000),
            ("DC", "dc_networks", "building_count", 40000, 50, 150000),
            ("MINNEAPOLIS", "minneapolis_networks", "building_count", 5000, 30, 30000),
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

if __name__ == "__main__":
    unittest.main()
