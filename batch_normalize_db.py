import os
import psycopg2
from io import StringIO
from api.shared_utils import normalize_person_name, normalize_business_name
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("batch_normalize")

DATABASE_URL = os.environ.get("DATABASE_URL")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False # Use transactions
    cur = conn.cursor()

    try:
        # 1. Normalize Principals
        logger.info("Normalizing names in 'principals' table...")
        cur.execute("SELECT id, name_c FROM principals")
        rows = cur.fetchall()
        
        output = StringIO()
        for pid, name in rows:
            norm = normalize_person_name(name)
            output.write(f"{pid}\t{norm}\n")
        output.seek(0)

        cur.execute("CREATE TEMP TABLE tmp_principals (id INT, norm TEXT)")
        cur.copy_from(output, 'tmp_principals', columns=('id', 'norm'))
        
        cur.execute("""
            UPDATE principals p
            SET name_c_norm = t.norm
            FROM tmp_principals t
            WHERE p.id = t.id
        """)
        logger.info(f"Updated {len(rows)} principals.")

        # 2. Normalize Properties Owners and Co-Owners
        logger.info("Normalizing names in 'properties' table...")
        cur.execute("SELECT id, owner, co_owner FROM properties WHERE (owner IS NOT NULL AND owner != 'Current Owner') OR (co_owner IS NOT NULL AND co_owner != '')")
        rows = cur.fetchall()
        
        output = StringIO()
        for pid, owner, co_owner in rows:
            owner_norm = normalize_business_name(owner) if owner else ""
            co_owner_norm = normalize_business_name(co_owner) if co_owner else ""
            output.write(f"{pid}\t{owner_norm}\t{co_owner_norm}\n")
        output.seek(0)

        cur.execute("CREATE TEMP TABLE tmp_properties (id INT, o_norm TEXT, co_norm TEXT)")
        cur.copy_from(output, 'tmp_properties', columns=('id', 'o_norm', 'co_norm'))
        
        cur.execute("""
            UPDATE properties p
            SET owner_norm = t.o_norm,
                co_owner_norm = t.co_norm
            FROM tmp_properties t
            WHERE p.id = t.id
        """)
        logger.info(f"Updated {len(rows)} properties (owner and co_owner).")

        conn.commit()
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
