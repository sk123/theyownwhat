
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def test_query():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # simplified query focusing on the count logic for network 1306
    query = """
    SELECT 
        network_id,
        (SELECT COUNT(*) FROM entity_networks en WHERE en.network_id = network_id AND en.entity_type = 'business') as business_count
    FROM (VALUES (1306)) as t(network_id)
    """
    
    cursor.execute(query)
    print("Direct Count Check:")
    print(cursor.fetchall())
    
    # Now the FULL TOP NETWORKS query structure but filtered for 1306
    full_query = """
        WITH property_to_network AS (
            -- Link properties via business_id
            SELECT p.id as property_id, en.network_id
            FROM properties p
            JOIN entity_networks en ON p.business_id::text = en.entity_id AND en.entity_type = 'business'
            WHERE p.business_id IS NOT NULL AND en.network_id = 1306
            
            UNION
            
            -- Link properties via principal_id
            SELECT p.id, en.network_id
            FROM properties p
            JOIN principals pr ON p.principal_id = pr.id::text
            JOIN entity_networks en ON pr.name_c = en.entity_id AND en.entity_type = 'principal'
            WHERE p.principal_id IS NOT NULL AND en.network_id = 1306

            UNION

            -- Link properties via owner_norm
            SELECT p.id, en.network_id
            FROM properties p
            JOIN entity_networks en ON p.owner_norm = en.entity_id AND en.entity_type = 'principal'
            WHERE p.owner_norm IS NOT NULL AND en.network_id = 1306
        ),
        top_networks AS (
            SELECT
                ptn.network_id,
                COUNT(DISTINCT ptn.property_id) as property_count,
                (SELECT COUNT(*) FROM entity_networks en WHERE en.network_id = ptn.network_id AND en.entity_type = 'business') as business_count
            FROM property_to_network ptn
            GROUP BY ptn.network_id
        )
        SELECT * FROM top_networks
    """
    
    cursor.execute(full_query)
    print("\nFull Query Check:")
    print(cursor.fetchall())
    
    conn.close()

if __name__ == "__main__":
    test_query()
