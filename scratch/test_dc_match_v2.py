import re
import pandas as pd
import psycopg2

def clean_address(addr):
    if not addr or not isinstance(addr, str):
        return ""
    addr = addr.upper()
    if 'WASHINGTON' in addr:
        addr = addr.split('WASHINGTON')[0]
    addr = re.sub(r'\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*', '', addr)
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
    addr = re.sub(r'[^A-Z0-9]', '', addr)
    return addr

def extract_street_words(addr):
    if not addr or not isinstance(addr, str):
        return set()
    addr = addr.upper()
    if 'WASHINGTON' in addr:
        addr = addr.split('WASHINGTON')[0]
    addr = re.sub(r'\b(SUITE|STE|APT|APARTMENT|UNIT|FLOOR|FL|#|DEPT)\b.*', '', addr)
    words = re.findall(r'\b[A-Z0-9]+\b', addr)
    # ignore numbers and short common words
    ignore = {'ST', 'AVE', 'RD', 'DR', 'CT', 'PL', 'BLVD', 'LN', 'TER', 'CIR', 'PKWY', 'NE', 'NW', 'SE', 'SW', 'N', 'S', 'E', 'W', 'STREET', 'AVENUE', 'ROAD'}
    return {w for w in words if w not in ignore and not w.isdigit()}

# Connect and load dc_properties
conn = psycopg2.connect('postgresql://user:password@localhost:5432/ctdata')
cur = conn.cursor()
cur.execute("SELECT bbl, address, owner_name, owner_name_norm FROM dc_properties")
db_props = cur.fetchall()
conn.close()

# Build maps
db_address_map = {}
db_owner_map = {} # owner_name_norm -> list of (bbl, address, street_words_set)
for bbl, address, owner_name, owner_name_norm in db_props:
    norm_addr = clean_address(address)
    if norm_addr:
        db_address_map[norm_addr] = bbl
    
    if owner_name_norm:
        if owner_name_norm not in db_owner_map:
            db_owner_map[owner_name_norm] = []
        db_owner_map[owner_name_norm].append((bbl, address, extract_street_words(address)))

# Read violations
df = pd.read_csv('data/dc_violations.csv')
df = df.iloc[1:] # skip row 0 which is total

matched_direct = 0
matched_owner_street = 0
unmatched = 0

for idx, row in df.iterrows():
    raw_addr = row['Full Address']
    raw_owner = row['Owner Fullname']
    
    norm_addr = clean_address(raw_addr)
    # 1. Direct address match
    if norm_addr in db_address_map:
        matched_direct += 1
        continue
        
    # 2. Owner + Street match fallback
    owner_norm = str(raw_owner).strip().upper() if pd.notna(raw_owner) else ""
    if owner_norm in db_owner_map:
        csv_street_words = extract_street_words(raw_addr)
        # Find properties of this owner that share street words
        candidates = []
        for bbl, db_addr, db_street_words in db_owner_map[owner_norm]:
            if csv_street_words & db_street_words:
                candidates.append(bbl)
        if len(candidates) == 1:
            matched_owner_street += 1
            continue
            
    unmatched += 1

total = len(df)
print(f"Total violations rows: {total}")
print(f"Matched direct address: {matched_direct} ({matched_direct/total*100:.2f}%)")
print(f"Matched via owner+street fallback: {matched_owner_street} ({matched_owner_street/total*100:.2f}%)")
print(f"Total matched: {matched_direct + matched_owner_street} ({(matched_direct+matched_owner_street)/total*100:.2f}%)")
print(f"Unmatched: {unmatched} ({unmatched/total*100:.2f}%)")
