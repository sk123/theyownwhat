import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Check temp_network_stats
            cur.execute("SELECT * FROM temp_network_stats WHERE id = 586")
            log.info("temp_network_stats for 586: %s", cur.fetchone())
            
            # 2. Check if 586 is in temp_principal_network_map
            cur.execute("SELECT COUNT(*) FROM temp_principal_network_map WHERE network_id = 586")
            log.info("Principals in network 586 map: %s", cur.fetchone()['count'])
            
            # 3. Check the Top Principals Query result for 586
            # (Re-running a simplified version of the compute_top_principals logic)
            cur.execute("""
                WITH network_aggregates AS (
                    SELECT pl.raw_pid, pl.network_id, ns.total_properties
                    FROM temp_principal_network_map pl
                    JOIN temp_network_stats ns ON ns.id = pl.network_id
                    WHERE ns.id = 586
                )
                SELECT pr.id, pr.name_c, na.total_properties
                FROM network_aggregates na
                JOIN principals pr ON pr.id::text = na.raw_pid
            """)
            rows = cur.fetchall()
            log.info("Principals found for 586: %s", len(rows))
            for r in rows[:5]:
                log.info("  - %s (%s properties)", r['name_c'], r['total_properties'])

if __name__ == "__main__":
    main()
