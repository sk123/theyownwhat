#!/usr/bin/env python3
"""
tests/test_gurevitch_linkage.py
================================
Automated regression test asserting that Menachem & Yehuda Gurevitch
are connected into a unified network component with 1,200+ properties.
"""

import os
import sys
import unittest
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

class TestGurevitchNetworkLinkage(unittest.TestCase):

    def setUp(self):
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
        except Exception:
            # Fallback to localhost if inside container or local host test
            alt_url = "postgresql://user:password@localhost:5432/ctdata"
            self.conn = psycopg2.connect(alt_url)

    def tearDown(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def test_gurevitch_unified_network(self):
        """Assert Menachem & Yehuda Gurevitch are in the exact same network with >= 1,200 properties."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find Menachem and Yehuda Gurevitch principal network records
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

            target_network_id = list(network_ids)[0]

            # Fetch network total properties
            cur.execute("""
                SELECT id, primary_name, total_properties, business_count, principal_count
                FROM networks
                WHERE id = %s
            """, (target_network_id,))
            net_row = cur.fetchone()
            self.assertIsNotNone(net_row, f"Network {target_network_id} not found in networks table")

            total_props = net_row.get("total_properties") or 0
            self.assertGreaterEqual(
                total_props, 1200,
                f"Gurevitch network property count dropped below threshold: found {total_props}, expected >= 1200"
            )

if __name__ == "__main__":
    unittest.main()
