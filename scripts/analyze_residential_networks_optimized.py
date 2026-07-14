
import os
import psycopg2
import pandas as pd

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def main():
    conn = get_db_connection()
    
    target_ids = {
        "MATTHEW FRAZIER": 5263,
        "MENACHEM GUREVITCH": 262,
        "JOSEPH F. CARABETTA JR.": 362,
        "DAVID A. PARISIER": 895,
        "GIDEON Z. FRIEDMAN": 4779,
        "DAVID A. MACK": 1976
    }
    
    results = []
    
    for name, nid in target_ids.items():
        print(f"Analyzing {name} (Network {nid})...")
        
        # 1. Get Entity IDs for this network
        entities_query = f"SELECT entity_id, entity_type FROM entity_networks WHERE network_id = {nid}"
        entities_df = pd.read_sql_query(entities_query, conn)
        
        bus_ids = entities_df[entities_df['entity_type'] == 'business']['entity_id'].tolist()
        princ_ids = entities_df[entities_df['entity_type'] == 'principal']['entity_id'].tolist()
        
        # 2. Build property query with IN clause (faster than join on OR)
        where_clauses = []
        if bus_ids:
            # handle single quote escaping if needed, but IDs are usually alphanumeric
            ids_str = "', '".join(bus_ids)
            where_clauses.append(f"business_id IN ('{ids_str}')")
        if princ_ids:
            ids_str = "', '".join(princ_ids)
            where_clauses.append(f"principal_id IN ('{ids_str}')")
            
        if not where_clauses:
            print(f"  No entities found for network {nid}")
            continue
            
        where_sql = " OR ".join(where_clauses)
        
        # Stats
        stats_query = f"""
        SELECT SUM(assessed_value) as total_value, COUNT(id) as property_count
        FROM properties
        WHERE {where_sql}
        """
        stats = pd.read_sql_query(stats_query, conn).iloc[0]
        
        # Cities
        cities_query = f"""
        SELECT property_city, COUNT(*) as c
        FROM properties
        WHERE {where_sql}
        GROUP BY property_city
        ORDER BY c DESC
        LIMIT 3
        """
        cities_df = pd.read_sql_query(cities_query, conn)
        top_cities = ", ".join([f"{r['property_city']} ({r['c']})" for _, r in cities_df.iterrows()])
        
        # LLCs
        bus_names_query = f"""
        SELECT DISTINCT entity_name 
        FROM entity_networks 
        WHERE network_id = {nid} AND entity_type = 'business'
        LIMIT 5;
        """
        bus_df = pd.read_sql_query(bus_names_query, conn)
        sample_llcs = ", ".join(bus_df['entity_name'].tolist())
        
        # Types
        types_query = f"""
        SELECT property_type, COUNT(*) as c
        FROM properties
        WHERE {where_sql}
        GROUP BY property_type
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
