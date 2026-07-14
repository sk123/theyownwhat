import psycopg2, os
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Repairing entity_networks from property_network_links...")

# 1. Properties -> Businesses
cur.execute("""
    INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name)
    SELECT DISTINCT pnl.network_id, 'business', p.business_id::text, MAX(b.name)
    FROM property_network_links pnl
    JOIN properties p ON p.id = pnl.property_id
    JOIN businesses b ON b.id = p.business_id
    WHERE p.business_id IS NOT NULL
    ON CONFLICT (network_id, entity_type, entity_id) DO NOTHING;
""")
print(f"Inserted {cur.rowcount} business links.")

# 2. Properties -> Principals
cur.execute("""
    INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name)
    SELECT DISTINCT pnl.network_id, 'principal', p.principal_id::text, MAX(pr.name_c)
    FROM property_network_links pnl
    JOIN properties p ON p.id = pnl.property_id
    JOIN principals pr ON pr.id = p.principal_id
    WHERE p.principal_id IS NOT NULL
    ON CONFLICT (network_id, entity_type, entity_id) DO NOTHING;
""")
print(f"Inserted {cur.rowcount} principal links.")

conn.commit()
print("Done repairing entity_networks.")
