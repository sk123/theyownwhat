
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def analyze_gaps():
    db_url = os.environ['DATABASE_URL']
    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("Analyzing municipalities for potential unit data gaps...")
        print("-" * 80)
        print(f"{'Municipality':<25} | {'Total Props':<12} | {'Has Unit':<10} | {'% Unit':<8} | {'Avg Dupes':<10}")
        print("-" * 80)
        
        # Methodology:
        # 1. Count total properties per city.
        # 2. Count properties with non-null units.
        # 3. Calculate "Duplication Factor": How many properties share the same address (without unit)? 
        #    High duplication + low unit count = High probability of missing unit numbers.
        
        query = """
        WITH city_stats AS (
            SELECT 
                property_city,
                COUNT(*) as total_count,
                COUNT(unit) as unit_count,
                COUNT(DISTINCT location) as distinct_address_count
            FROM properties
            GROUP BY property_city
        )
        SELECT 
            property_city,
            total_count,
            unit_count,
            Round(unit_count::decimal / NULLIF(total_count, 0) * 100, 1) as unit_pct,
            Round(total_count::decimal / NULLIF(distinct_address_count, 0), 2) as avg_dupes
        FROM city_stats
        WHERE total_count > 500
        ORDER BY avg_dupes DESC, total_count DESC
        LIMIT 50;
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        for row in rows:
            dupes = row['avg_dupes'] if row['avg_dupes'] is not None else 0.0
            print(f"{row['property_city']:<25} | {row['total_count']:<12} | {row['unit_count']:<10} | {row['unit_pct']:<8} | {dupes:<10}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_gaps()
