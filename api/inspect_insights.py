import psycopg2, os
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get('DATABASE_URL')
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

print('TOP 20 STATEWIDE NETWORKS (from cached_insights):')
cur.execute('''
    SELECT network_name, property_count, total_assessed_value 
    FROM cached_insights 
    ORDER BY property_count DESC 
    LIMIT 20
''')
for r in cur.fetchall():
    val = float(r['total_assessed_value'] or 0) / 1000000
    print(f"  - {r['network_name']}: {r['property_count']} properties (${val:,.1f}M)")

print('\nSEARCHING FOR GUREVITCH IN INSIGHTS:')
cur.execute('''
    SELECT network_name, property_count, total_assessed_value 
    FROM cached_insights 
    WHERE network_name ILIKE '%GUREVITCH%' OR network_name ILIKE '%GUREVICH%'
''')
for r in cur.fetchall():
    val = float(r['total_assessed_value'] or 0) / 1000000
    print(f"  - {r['network_name']}: {r['property_count']} properties (${val:,.1f}M)")

conn.close()
