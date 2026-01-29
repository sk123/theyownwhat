
import os
import psycopg2
import logging
from psycopg2.extras import RealDictCursor, execute_values

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Direct DB connection
def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    return psycopg2.connect(db_url)

# Import the calculation logic (assuming it doesn't need db_pool at import time)
# We need to mock db_pool inside api.main before importing if the import triggers something
# But based on previous check, db_pool is None by default.

# The function _update_insights_cache_sync in api/main.py uses db_pool.getconn()
# So we can't use that function easily.
# But we CAN usage _calculate_and_cache_insights if we pass it a cursor.

from api.main import _calculate_and_cache_insights

def refresh_insights():
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        insights_by_municipality = {}
        all_rows_for_table = []
        
        # 1. Statewide
        logger.info("Calculating STATEWIDE...")
        statewide_data = _calculate_and_cache_insights(cursor, None, None)
        if statewide_data:
            insights_by_municipality['STATEWIDE'] = statewide_data
            for i, net in enumerate(statewide_data):
                all_rows_for_table.append({
                    'city': 'Statewide',
                    'rank': i + 1,
                    'data': net
                })

        # 2. Major Cities
        major_cities = ['Bridgeport', 'New Haven', 'Hartford', 'Stamford', 'Waterbury', 'Norwalk', 'Danbury', 'New Britain']
        for t in major_cities:
            logger.info(f"Calculating {t}...")
            town_col = 'property_city'
            town_networks = _calculate_and_cache_insights(cursor, town_col, t)
            if town_networks:
                insights_by_municipality[t.upper()] = town_networks
                for i, net in enumerate(town_networks):
                    all_rows_for_table.append({
                        'city': t,
                        'rank': i + 1,
                        'data': net
                    })

        # 3. Update KV Cache (for Frontend Cards)
        import json
        from api.main import json_converter
        logger.info("Updating kv_cache (JSON)...")
        insights_json = json.dumps(insights_by_municipality, default=json_converter)
        cursor.execute("""
            INSERT INTO kv_cache (key, value) VALUES (%s, %s::jsonb)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = now()
        """, ('insights', insights_json))

        # 4. Update Cached Insights Table (for Header Lookup / Search)
        logger.info("Updating cached_insights table...")
        cursor.execute("TRUNCATE TABLE cached_insights")
        
        insert_query = """
            INSERT INTO cached_insights (
                title, rank, network_name, property_count, 
                total_assessed_value, total_appraised_value,
                primary_entity_id, primary_entity_name, primary_entity_type,
                business_count, controlling_business, representative_entities, created_at
            ) VALUES %s
        """
        
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        
        values = []
        for row in all_rows_for_table:
            net = row['data']
            val = (
                row['city'],
                row['rank'],
                net['entity_name'],
                net['value'],
                net['total_assessed_value'],
                net['total_appraised_value'],
                net['entity_id'],
                net['entity_name'],
                net['entity_type'],
                net['business_count'],
                net.get('controlling_business_name'),
                psycopg2.extras.Json(net.get('representative_entities', [])),
                now
            )
            values.append(val)
            
        if values:
            execute_values(cursor, insert_query, values)
            
        conn.commit()
        logger.info("✅ All caches updated successfully.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    refresh_insights()
