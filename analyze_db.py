import os
import psycopg2
from psycopg2.extras import RealDictCursor

def analyze_db():
    database_url = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/ctdata")
    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            property_city,
            COUNT(*) as total_properties,
            COUNT(*) FILTER (WHERE owner ILIKE '%Current Owner%' OR owner IS NULL OR owner = '') as missing_owner,
            COUNT(*) FILTER (WHERE appraised_value IS NULL OR appraised_value = 0) as missing_appraisal,
            COUNT(*) FILTER (WHERE assessed_value IS NULL OR assessed_value = 0) as missing_assessment,
            COUNT(*) FILTER (WHERE cama_site_link IS NULL OR cama_site_link = '') as missing_link
        FROM properties
        GROUP BY property_city
        ORDER BY missing_owner DESC, total_properties DESC
        LIMIT 30;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        print(f"{'City':<25} | {'Total':<8} | {'No Owner':<8} | {'No Appr':<8} | {'No Link':<8}")
        print("-" * 70)
        for row in results:
            print(f"{row['property_city']:<25} | {row['total_properties']:<8} | {row['missing_owner']:<8} | {row['missing_appraisal']:<8} | {row['missing_link']:<8}")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_db()
