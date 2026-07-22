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

if __name__ == "__main__":
    unittest.main()
