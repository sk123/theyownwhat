
import os
import psycopg2
import json
from psycopg2.extras import RealDictCursor
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    return psycopg2.connect(db_url)

def repair_kv_cache():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            logger.info("Reading cached_insights...")
            cur.execute("""
                SELECT title, rank, network_name, property_count, total_assessed_value, total_appraised_value,
                       primary_entity_id, primary_entity_name, primary_entity_type, business_count, principal_count, building_count, unit_count, representative_entities, controlling_business, linked_business_count
                FROM cached_insights
                ORDER BY title, rank
            """)
            all_rows = cur.fetchall()
            
            insights_map = {}
            for r in all_rows:
                group = r['title']
                if group not in insights_map:
                    insights_map[group] = []
                
                item = {
                    "rank": r['rank'],
                    "entity_name": r['network_name'],
                    "entity_id": r['primary_entity_id'],
                    "entity_type": r['primary_entity_type'],
                    "value": int(r['unit_count'] or 0),
                    "property_count": r['property_count'],
                    "business_count": r['business_count'],
                    "principal_count": r.get('principal_count', 0),
                    "building_count": r['building_count'],
                    "unit_count": r['unit_count'],
                    "total_assessed_value": float(r['total_assessed_value'] or 0),
                    "total_appraised_value": float(r['total_appraised_value'] or 0),
                    "representative_entities": r['representative_entities'],
                    "principals": r.get('principals', []),
                    "controlling_business": r['controlling_business'],
                    "linked_business_count": r.get('linked_business_count', 0)
                }
                insights_map[group].append(item)
            
            logger.info(f"Updating kv_cache with {len(insights_map)} groups...")
            cur.execute("""
                INSERT INTO kv_cache (key, value)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    created_at = now();
            """, ('insights', json.dumps(insights_map)))
            
            conn.commit()
            logger.info("✅ kv_cache repaired.")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    repair_kv_cache()
