import re
import pandas as pd
import psycopg2

def clean_address(addr):
    if not addr or not isinstance(addr, str):
        return ""
    # uppercase
    addr = addr.upper()
    # strip anything after 'WASHINGTON'
    if 'WASHINGTON' in addr:
        addr = addr.split('WASHINGTON')[0]
    # strip unit patterns
    addr = re.sub(r'\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*', '', addr)
    # replace suffixes
    replacements = {
        'STREET': 'ST',
        'AVENUE': 'AVE',
        'ROAD': 'RD',
        'DRIVE': 'DR',
        'COURT': 'CT',
        'PLACE': 'PL',
        'BOULEVARD': 'BLVD',
        'LANE': 'LN',
        'TERRACE': 'TER',
        'CIRCLE': 'CIR',
        'PARKWAY': 'PKWY',
        'PKY': 'PKWY',
    }
    for k, v in replacements.items():
        addr = re.sub(r'\b' + k + r'\b', v, addr)
    # strip all non-alphanumeric
    addr = re.sub(r'[^A-Z0-9]', '', addr)
    return addr

# Connect and load dc_properties
conn = psycopg2.connect('postgresql://user:password@localhost:5432/ctdata')
cur = conn.cursor()
cur.execute("SELECT bbl, address, owner_name FROM dc_properties")
db_props = cur.fetchall()
conn.close()

# Build map
db_map = {}
for bbl, address, owner_name in db_props:
    norm = clean_address(address)
    if norm:
        db_map[norm] = (bbl, address, owner_name)

print("Total DB properties mapped:", len(db_map))

# Read violations
df = pd.read_csv('data/dc_violations.csv')
# Skip row 0 which is total
df = df.iloc[1:]

matched = 0
unmatched_addresses = []
for idx, row in df.iterrows():
    raw_addr = row['Full Address']
    norm_addr = clean_address(raw_addr)
    if norm_addr in db_map:
        matched += 1
    else:
        unmatched_addresses.append((raw_addr, norm_addr))

print("Total violations rows:", len(df))
print("Matched violations rows:", matched)
print("Match rate: {:.2f}%".format(matched / len(df) * 100))

print("\nSome unmatched examples:")
for raw, norm in unmatched_addresses[:20]:
    print(f"Raw: {raw} | Norm: {norm}")
