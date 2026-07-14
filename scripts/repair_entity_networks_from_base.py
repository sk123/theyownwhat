import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

print("Repairing entity_networks from businesses & principals...")

cur.execute("""
    INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name)
    SELECT network_id, 'business', id::text, name
    FROM businesses
    WHERE network_id IS NOT NULL
    ON CONFLICT (network_id, entity_type, entity_id) DO NOTHING;
""")
print(f"Inserted {cur.rowcount} business links.")

cur.execute("""
    INSERT INTO entity_networks (network_id, entity_type, entity_id, entity_name)
    SELECT network_id, 'principal', id::text, name_c
    FROM principals
    WHERE network_id IS NOT NULL
    ON CONFLICT (network_id, entity_type, entity_id) DO NOTHING;
""")
print(f"Inserted {cur.rowcount} principal links.")

conn.commit()
print("Done repairing entity_networks.")
