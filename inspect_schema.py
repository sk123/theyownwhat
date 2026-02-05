import sqlite3

conn = sqlite3.connect('api/properties.db')
cursor = conn.cursor()

def print_schema(table):
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
    schema = cursor.fetchone()
    if schema:
        print(f"\n--- {table} Schema ---")
        print(schema[0])
    else:
        print(f"\nTable {table} not found")

print_schema('principal')
print_schema('business')
conn.close()
