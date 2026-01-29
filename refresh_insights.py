import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from api.main import _calculate_and_cache_insights, json_converter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")

def force_refresh():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            logger.info("Starting manual insights refresh...")
            
            insights_by_municipality = {}
            insights_by_municipality['STATEWIDE'] = _calculate_and_cache_insights(cursor, None, None, sort_mode='total')
            insights_by_municipality['STATEWIDE_SUBSIDIZED'] = _calculate_and_cache_insights(cursor, None, None, sort_mode='subsidized')
            
            major_cities = ['Bridgeport', 'New Haven', 'Hartford', 'Stamford', 'Waterbury', 'Norwalk', 'Danbury', 'New Britain']
            for t in major_cities:
                town_col = 'property_city'
                # Standard
                town_networks = _calculate_and_cache_insights(cursor, town_col, t, sort_mode='total')
                if town_networks:
                    insights_by_municipality[t.upper()] = town_networks
                
                # Subsidized
                try:
                    sub_networks = _calculate_and_cache_insights(cursor, town_col, t, sort_mode='subsidized')
                    if sub_networks:
                        insights_by_municipality[f"{t.upper()}_SUBSIDIZED"] = sub_networks
                except Exception as e:
                    logger.error(f"Failed to calc subsidized for {t}: {e}")
            
            insights_json = json.dumps(insights_by_municipality, default=json_converter)
            
            cursor.execute("""
                INSERT INTO kv_cache (key, value) VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = now()
            """, ('insights', insights_json))
            
            logger.info("✅ Manual insights refresh complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    force_refresh()
