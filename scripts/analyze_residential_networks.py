
import os
import psycopg2
import pandas as pd

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def main():
    conn = get_db_connection()
    
    # IDs collected so far:
    # Frazier: 5263
    # Gurevitch: 262
    # Carabetta: 362
    # Parisier: 895
    # Friedman: ?
    # Mack: ?
    
    # I will fill these in dynamically or run this script after getting the last 2.
    # For now, let's assume we pass them as args or hardcode the ones we have + placeholders.
    
    # Wait, I can just query by ID in the loop if I have them.
    # Let's start with the ones we have to save time, and add the others if they finish.
    
    target_ids = {
        "MATTHEW FRAZIER": 5263,
        "MENACHEM GUREVITCH": 262,
        "JOSEPH F. CARABETTA JR.": 362,
        "DAVID A. PARISIER": 895,
        "GIDEON Z. FRIEDMAN": 4779, # From previous output!
        "DAVID A. MACK": 1976 # From previous output!
    }
    
    # Wait, I already saw 4779 and 1976 in the PREVIOUS script output (Step 22066)!
    # I don't need to wait for the slow queries!
    # network_id |         entity_name         | entity_type 
    #        1976 | DAVID A. MACK               | principal
    #        4779 | GIDEON Z. FRIEDMAN          | principal
    
    # I HAVE THEM ALL.
    
    results = []
    
    for name, nid in target_ids.items():
        print(f"Analyzing {name} (Network {nid})...")
        
        # Stats
        stats_query = f"""
        SELECT SUM(p.assessed_value) as total_value, COUNT(p.id) as property_count
        FROM properties p
        JOIN entity_networks en ON (
            (en.entity_type = 'business' AND en.entity_id = p.business_id) OR
            (en.entity_type = 'principal' AND en.entity_id = p.principal_id)
        )
        WHERE en.network_id = {nid}
        """
        stats = pd.read_sql_query(stats_query, conn).iloc[0]
        
        # Cities
        cities_query = f"""
        SELECT p.property_city, COUNT(*) as c
        FROM properties p
        JOIN entity_networks en ON (
            (en.entity_type = 'business' AND en.entity_id = p.business_id) OR
            (en.entity_type = 'principal' AND en.entity_id = p.principal_id)
        )
        WHERE en.network_id = {nid}
        GROUP BY p.property_city
        ORDER BY c DESC
        LIMIT 3
        """
        cities_df = pd.read_sql_query(cities_query, conn)
        top_cities = ", ".join([f"{r['property_city']} ({r['c']})" for _, r in cities_df.iterrows()])
        
        # LLCs
        entities_query = f"""
        SELECT DISTINCT entity_name 
        FROM entity_networks 
        WHERE network_id = {nid} AND entity_type = 'business'
        LIMIT 5;
        """
        entities_df = pd.read_sql_query(entities_query, conn)
        sample_llcs = ", ".join(entities_df['entity_name'].tolist())
        
        # Types
        types_query = f"""
        SELECT p.property_type, COUNT(*) as c
        FROM properties p
        JOIN entity_networks en ON (
            (en.entity_type = 'business' AND en.entity_id = p.business_id) OR
            (en.entity_type = 'principal' AND en.entity_id = p.principal_id)
        )
        WHERE en.network_id = {nid}
        GROUP BY p.property_type
        ORDER BY c DESC
        LIMIT 3
        """
        types_df = pd.read_sql_query(types_query, conn)
        top_types = ", ".join([f"{r['property_type']} ({r['c']})" for _, r in types_df.iterrows()])

        results.append({
            "Name": name,
            "Value": stats['total_value'],
            "Count": stats['property_count'],
            "Cities": top_cities,
            "LLCs": sample_llcs,
            "Types": top_types
        })

    # Sort by Value
    results.sort(key=lambda x: x['Value'] or 0, reverse=True)
    
    print("\n\n=== RESIDENTIAL DEEP DIVE ===\n")
    for r in results:
        val = f"${r['Value']:,.0f}" if r['Value'] else "$0"
        print(f"Name: {r['Name']}")
        print(f"  Value: {val}")
        print(f"  Count: {r['Count']}")
        print(f"  Cities: {r['Cities']}")
        print(f"  Types: {r['Types']}")
        print(f"  LLCs: {r['LLCs']}")
        print("-" * 30)

if __name__ == "__main__":
    main()
