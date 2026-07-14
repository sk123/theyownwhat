
import os
import psycopg2
import pandas as pd

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@ctdata_db:5432/ctdata")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def main():
    conn = get_db_connection()
    

    # Target Principals from Frontend Screenshot
    targets = [
        "GIDEON Z. FRIEDMAN",
        "DAVID A. MACK",
        "MATTHEW FRAZIER",
        "MENACHEM GUREVITCH",
        "JOSEPH F. CARABETTA JR.",
        "DAVID A. PARISIER"
    ]
    
    # 2. Find their Network IDs (Largest by Value)
    print("Finding Networks for Target Principals...")
    
    target_networks = []
    
    for target in targets:
        # Fuzzy match principal name to find largest network
        # We search entity_networks for name like target, pick the one with highest stats
        
        # Note: Names might be normalized differently in DB (e.g. middle initial)
        # We'll use ILIKE
        
        search_term = target.replace(".", "").split(" ")[0] # "GIDEON", "DAVID", etc.
        # Better: use parts.
        parts = target.split()
        if len(parts) > 1:
            term = f"%{parts[0]}%{parts[-1]}%"
        else:
            term = f"%{parts[0]}%"
            
        print(f"  Searching for {target} (term: {term})...")
        
        query = f"""
        SELECT 
            en.network_id,
            SUM(p.assessed_value) as total_value,
            COUNT(p.id) as property_count
        FROM properties p
        JOIN entity_networks en ON (
            (en.entity_type = 'business' AND en.entity_id = p.business_id) OR
            (en.entity_type = 'principal' AND en.entity_id = p.principal_id)
        )
        WHERE en.entity_name ILIKE '{term}' AND en.entity_type = 'principal'
        GROUP BY en.network_id
        ORDER BY total_value DESC
        LIMIT 1;
        """
        
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            nid = df.iloc[0]['network_id']
            val = df.iloc[0]['total_value']
            count = df.iloc[0]['property_count']
            print(f"    Found Network {nid}: ${val:,.0f} ({count} props)")
            target_networks.append((target, nid))
        else:
            print(f"    No network found for {target}")

    # 3. Deep Dive Loop for Targets
    results = []
    
    for original_name, nid in target_networks:
        
        
        # Get details (same logic as before)
        # Top Cities
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
        
        # Sample Entity Names (LLCs)
        entities_query = f"""
        SELECT DISTINCT entity_name 
        FROM entity_networks 
        WHERE network_id = {nid} AND entity_type = 'business'
        LIMIT 5;
        """
        entities_df = pd.read_sql_query(entities_query, conn)
        sample_llcs = ", ".join(entities_df['entity_name'].tolist())
        
        # Principal Name (Most common normalized owner)
        principal_query = f"""
        SELECT p.owner_norm, COUNT(*) as c
        FROM properties p
        JOIN entity_networks en ON (
            (en.entity_type = 'business' AND en.entity_id = p.business_id) OR
            (en.entity_type = 'principal' AND en.entity_id = p.principal_id)
        )
        WHERE en.network_id = {nid}
        GROUP BY p.owner_norm
        ORDER BY c DESC
        LIMIT 1
        """
        principal_df = pd.read_sql_query(principal_query, conn)
        
        # Stats recalculation for accuracy (since we searched on specific entity before)
        stats_query = f"""
        SELECT SUM(p.assessed_value) as total_value, COUNT(p.id) as property_count
        FROM properties p
        JOIN entity_networks en ON (
            (en.entity_type = 'business' AND en.entity_id = p.business_id) OR
            (en.entity_type = 'principal' AND en.entity_id = p.principal_id)
        )
        WHERE en.network_id = {nid}
        """
        stats_df = pd.read_sql_query(stats_query, conn)
        total_val = stats_df.iloc[0]['total_value']
        count = stats_df.iloc[0]['property_count']

        principal_name = principal_df.iloc[0]['owner_norm'] if not principal_df.empty else original_name
        
        # Property Types
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
            "Target": original_name,
            "Network ID": nid,
            "Principal/Name": principal_name,
            "Total Value": f"${total_val:,.0f}",
            "Properties": count,
            "Top Cities": top_cities,
            "Sample LLCs": sample_llcs,
            "Property Types": top_types
        })

    # Output Report
    print("\n\n=== RESIDENTIAL OWNERSHIP DEEP DIVE REPORT ===\n")
    for r in results:
        print(f"Target: {r['Target']}")
        print(f"  Principal: {r['Principal/Name']}")
        print(f"  Total Assessed Value: {r['Total Value']}")
        print(f"  Property Count: {r['Properties']}")
        print(f"  Top Cities: {r['Top Cities']}")
        print(f"  Primary Types: {r['Property Types']}")
        print(f"  Entities: {r['Sample LLCs']}")
        print("-" * 40)

if __name__ == "__main__":
    main()
