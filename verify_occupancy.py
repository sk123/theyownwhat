import os
import psycopg2
from psycopg2.extras import RealDictCursor

def verify_occupancy():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL not set")
        return

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("Checking relationship between property_type and number_of_units...")
        query = """
        SELECT property_type, number_of_units, COUNT(*) 
        FROM properties 
        WHERE number_of_units IS NOT NULL AND property_type IS NOT NULL
        GROUP BY property_type, number_of_units
        HAVING COUNT(*) > 10
        ORDER BY property_type, number_of_units;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        print(f"{'Property Type':<30} | {'Units':<5} | {'Count':<8}")
        print("-" * 50)
        for row in results:
            print(f"{str(row['property_type']):<30} | {str(row['number_of_units']):<5} | {row['count']:<8}")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_occupancy()
