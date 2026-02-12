#!/usr/bin/env python3
"""
Script to investigate the Gurevitch network issues:
1. Check cached_insights data
2. Check entity_networks data
3. Compare what's shown in insights vs what's loaded in network view
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    
    print("="*80)
    print("1. CHECKING CACHED_INSIGHTS TOP 5 NETWORKS")
    print("="*80)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT title, rank, network_name, primary_entity_name, primary_entity_type, 
                   controlling_business, property_count
            FROM cached_insights 
            WHERE title = 'STATEWIDE' 
            ORDER BY rank 
            LIMIT 5
        """)
        for row in cur.fetchall():
            print(f"Rank {row['rank']}: {row['network_name']} (entity: {row['primary_entity_name']}, type: {row['primary_entity_type']})")
            print(f"  Controlling Business: {row['controlling_business']}")
            print(f"  Properties: {row['property_count']}")
    
    print("\n" + "="*80)
    print("2. CHECKING GUREVITCH NETWORKS IN CACHED_INSIGHTS")
    print("="*80)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT title, rank, network_name, primary_entity_name, primary_entity_type
            FROM cached_insights 
            WHERE network_name ILIKE '%GUREVITCH%'
            ORDER BY title, rank
        """)
        for row in cur.fetchall():
            print(f"{row['title']} - Rank {row['rank']}: {row['network_name']} (type: {row['primary_entity_type']})")
    
    print("\n" + "="*80)
    print("3. CHECKING ENTITY_NETWORKS FOR GUREVITCH")
    print("="*80)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT network_id, entity_type, entity_id, entity_name, normalized_name
            FROM entity_networks 
            WHERE entity_name ILIKE '%GUREVITCH%'
            ORDER BY network_id, entity_type
        """)
        for row in cur.fetchall():
            print(f"Network {row['network_id']}: {row['entity_type']} - {row['entity_name']} (ID: {row['entity_id']})")
    
    print("\n" + "="*80)
    print("4. CHECKING NETWORKS TABLE FOR GUREVITCH")
    print("="*80)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT n.id, n.primary_name, n.total_properties, n.business_count, n.principal_count
            FROM networks n
            WHERE n.primary_name ILIKE '%GUREVITCH%'
            ORDER BY n.id
        """)
        for row in cur.fetchall():
            print(f"Network {row['id']}: {row['primary_name']}")
            print(f"  Properties: {row['total_properties']}, Businesses: {row['business_count']}, Principals: {row['principal_count']}")
    
    print("\n" + "="*80)
    print("5. CHECKING WHAT NETWORKS MENACHEM AND YEHUDA ARE IN")
    print("="*80)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check for Menachem
        cur.execute("""
            SELECT DISTINCT network_id
            FROM entity_networks
            WHERE entity_name ILIKE '%MENACHEM%GUREVITCH%'
        """)
        menachem_nets = [r['network_id'] for r in cur.fetchall()]
        print(f"Menachem is in networks: {menachem_nets}")
        
        # Check for Yehuda  
        cur.execute("""
            SELECT DISTINCT network_id
            FROM entity_networks
            WHERE entity_name ILIKE '%YEHUDA%GUREVITCH%'
        """)
        yehuda_nets = [r['network_id'] for r in cur.fetchall()]
        print(f"Yehuda is in networks: {yehuda_nets}")
        
        print(f"\nAre they in the same network? {set(menachem_nets) & set(yehuda_nets)}")
    
    conn.close()

if __name__ == "__main__":
    main()
