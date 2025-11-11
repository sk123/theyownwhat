# test_networks.py
import os
import sys
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

# Add the current directory to Python path to import shared_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from shared_utils import get_name_variations, normalize_business_name, normalize_person_name
except ImportError:
    print("Error: Could not import from shared_utils.py.")
    print("Please make sure this script is in the same directory as shared_utils.py.")
    sys.exit(1)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable is not set.")
    sys.exit(1)

def get_db_connection():
    """Establishes a simple, direct connection to the database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"FATAL: Database connection failed: {e}")
        sys.exit(1)

def find_network_by_name(conn, search_name: str):
    """Finds a network ID by searching businesses and principals."""
    network_id = None
    entity_type = None
    
    norm_biz = normalize_business_name(search_name)
    norm_person = normalize_person_name(search_name)

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        print(f"Searching for network matching normalized names: '{norm_biz}' OR '{norm_person}'")
        
        cursor.execute("""
            SELECT network_id, entity_type FROM entity_networks
            WHERE normalized_name = %s OR normalized_name = %s
            ORDER BY entity_type DESC
            LIMIT 1;
        """, (norm_biz, norm_person))
        
        result = cursor.fetchone()
        
        if result:
            print(f"Found match in entity_networks: Type '{result['entity_type']}', Network ID: {result['network_id']}")
            return result['network_id']
        else:
            print("No direct match in entity_networks. This entity may be isolated.")
            return None

def main():
    parser = argparse.ArgumentParser(description="Test network discovery from the command line.")
    parser.add_argument("name", type=str, help="The business or principal name to search for.")
    # --- NEW: Optional stats flag ---
    parser.add_argument('--stats', action='store_true', help="Show summary statistics only.")
    args = parser.parse_args()

    conn = get_db_connection()
    try:
        network_id = find_network_by_name(conn, args.name)
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if network_id:
                # --- NETWORK FOUND ---
                print("\n" + "="*30)
                print(f"NETWORK {network_id}: GATHERING DATA")
                print("="*30)
                
                # 1. Get all entities in the network
                cursor.execute("""
                    SELECT entity_type, entity_name, entity_id 
                    FROM entity_networks
                    WHERE network_id = %s
                    ORDER BY entity_type, entity_name
                """, (network_id,))
                entities = cursor.fetchall()
                
                # 2. Get all business IDs and principal names from the entity list
                biz_ids = tuple(e['entity_id'] for e in entities if e['entity_type'] == 'business')
                prin_ids = tuple(e['entity_id'] for e in entities if e['entity_type'] == 'principal')
                
                biz_count = len(biz_ids)
                prin_count = len(prin_ids)
                
                # 3. Get all properties linked to ANY of those entities (This mimics the new API logic)
                properties = []
                total_value = 0
                if biz_ids or prin_ids:
                    query_parts = []
                    params = []
                    if biz_ids:
                        query_parts.append("business_id IN %s")
                        params.append(biz_ids)
                    if prin_ids:
                        query_parts.append("principal_id IN %s")
                        params.append(prin_ids)
                    
                    full_query = f"""
                        SELECT location, property_city, owner, assessed_value, business_id, principal_id 
                        FROM properties
                        WHERE {' OR '.join(query_parts)}
                        ORDER BY property_city, location
                    """
                    cursor.execute(full_query, tuple(params))
                    properties = cursor.fetchall()
                    for prop in properties:
                        total_value += (prop['assessed_value'] or 0)
                
                # 4. Show output based on --stats flag
                if args.stats:
                    print("\n--- SUMMARY STATISTICS ---")
                    print(f"  Total Entities:   {len(entities)}")
                    print(f"    - Businesses: {biz_count}")
                    print(f"    - Principals: {prin_count}")
                    print(f"  Total Properties: {len(properties)}")
                    print(f"  Total Value:      ${total_value:,.0f}")
                else:
                    print("\n--- ENTITIES ---")
                    for entity in entities:
                        print(f"  [{entity['entity_type']}] {entity['entity_name']} (ID: {entity['entity_id']})")
                    print(f"\nFound {len(entities)} total entities.")
                    
                    print("\n--- PROPERTIES ---")
                    for prop in properties:
                        link_info = f"(Linked via Biz: {prop['business_id']})" if prop['business_id'] else f"(Linked via Prin: {prop['principal_id']})"
                        print(f"  [{prop['property_city']}] {prop['location']} (Owner: {prop['owner']}) - ${prop['assessed_value']:,.0f} {link_info}")
                    print(f"\nFound {len(properties)} total properties.")
                    print(f"Total Assessed Value: ${total_value:,.0f}")

            else:
                # --- ISOLATED ENTITY (LOGIC MODIFIED) ---
                print("\n" + "="*30)
                print(f"ISOLATED ENTITY: {args.name}")
                print("="*30)
                
                # Check properties table for direct links
                norm_biz = normalize_business_name(args.name)
                norm_prin = normalize_person_name(args.name)
                
                cursor.execute("""
                    SELECT location, property_city, owner, assessed_value 
                    FROM properties
                    WHERE business_id = (SELECT id FROM businesses WHERE normalized_name = %s LIMIT 1)
                       OR principal_id IN (%s, %s)
                """, (norm_biz, args.name.upper(), norm_prin))

                properties = cursor.fetchall()
                total_value = 0
                for prop in properties:
                    total_value += (prop['assessed_value'] or 0)

                if args.stats:
                    print("\n--- SUMMARY STATISTICS (ISOLATED) ---")
                    print(f"  Total Entities:   1")
                    print(f"  Total Properties: {len(properties)}")
                    print(f"  Total Value:      ${total_value:,.0f}")
                else:
                    print("No network found. Showing properties linked directly to this entity:")
                    for prop in properties:
                        print(f"  [{prop['property_city']}] {prop['location']} (Owner: {prop['owner']}) - ${prop['assessed_value']:,.0f}")
                    print(f"\nFound {len(properties)} total properties.")
                    print(f"Total Assessed Value: ${total_value:,.0f}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()