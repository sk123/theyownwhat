
import psycopg2
from psycopg2.extras import RealDictCursor

def get_low_quality_munis():
    try:
        conn = psycopg2.connect(
            dbname="ctdata",
            user="user",
            password="password",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for towns with properties but low photo coverage (< 10%)
        # or towns that exist in config but have 0 properties
        query = """
            SELECT 
                property_city, 
                COUNT(*) as total, 
                COUNT(building_photo) as photos,
                ROUND(COUNT(building_photo)::decimal / NULLIF(COUNT(*), 0) * 100, 1) as photo_pct
            FROM properties 
            WHERE property_city IS NOT NULL 
            GROUP BY property_city 
            HAVING COUNT(*) > 0 AND (COUNT(building_photo)::decimal / COUNT(*)) < 0.1
            ORDER BY total DESC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        print("--- Candidates for Scrape (Low Photo Coverage) ---")
        targets = []
        for r in rows:
            print(f"{r['property_city']}: {r['total']} props, {r['photo_pct']}% photos")
            targets.append(r['property_city'])
            
        return targets
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    get_low_quality_munis()
