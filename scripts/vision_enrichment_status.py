#!/usr/bin/env python3
"""
Vision Data Enrichment Status Monitor
Check progress of data enrichment across priority municipalities
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

DATABASE_URL = "postgresql://user:password@localhost:5432/ctdata"

PRIORITY_TOWNS = [
    'EAST HAMPTON',
    'CROMWELL', 
    'RIDGEFIELD',
    'CHESHIRE',
    'HAMDEN',
    'CLINTON',
    'OLD LYME'
]

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("=" * 100)
    print("VISION DATA ENRICHMENT STATUS")
    print("=" * 100)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Overall stats
    cur.execute('''
        SELECT 
            COUNT(*) as total_properties,
            COUNT(CASE WHEN owner = 'Current Owner' THEN 1 END) as current_owner_count,
            COUNT(CASE WHEN unit IS NOT NULL AND unit != '' THEN 1 END) as has_unit
        FROM properties
    ''')
    overall = cur.fetchone()
    
    pct_current = (overall['current_owner_count'] / overall['total_properties'] * 100) if overall['total_properties'] > 0 else 0
    pct_units = (overall['has_unit'] / overall['total_properties'] * 100) if overall['total_properties'] > 0 else 0
    
    print(f"STATEWIDE SUMMARY:")
    print(f"  Total Properties:     {overall['total_properties']:>10,}")
    print(f"  'Current Owner':      {overall['current_owner_count']:>10,} ({pct_current:>5.2f}%)")
    print(f"  With Unit Numbers:    {overall['has_unit']:>10,} ({pct_units:>5.2f}%)")
    print()
    print("=" * 100)
    
    # Priority towns detail
    print(f"{'Town':<20} {'Total':>8} {'Current Owner':>14} {'% CO':>6} {'Missing Unit':>13} {'Status':>8}")
    print("-" * 100)
    
    for town in PRIORITY_TOWNS:
        cur.execute('''
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN owner = 'Current Owner' THEN 1 END) as current_owner,
                COUNT(CASE WHEN unit IS NULL OR unit = '' THEN 1 END) as missing_unit
            FROM properties
            WHERE UPPER(property_city) = UPPER(%s)
        ''', (town,))
        
        row = cur.fetchone()
        if row and row['total'] > 0:
            pct_co = (row['current_owner'] / row['total'] * 100)
            
            # Color code by status (using emojis)
            status = "✓" if row['current_owner'] < 100 else "⚠"  if row['current_owner'] < 1000 else "✗"
            
            print(f"{town:<20} {row['total']:>8,} {row['current_owner']:>14,} {pct_co:>5.1f}% {row['missing_unit']:>13,} {status}")
    
    print("=" * 100)
    print()
    
    conn.close()

if __name__ == '__main__':
    main()
