import requests

# 1. Fetch owners
print("Fetching owners...")
owners_url = "https://data.cityofchicago.org/resource/ezma-pppn.json"
r = requests.get(owners_url, params={'$limit': 2000})
owners_data = r.json()
owners_by_account = {}
for row in owners_data:
    acc = row.get('account_number')
    if acc:
        name = row.get('owner_name') or f"{row.get('owner_first_name', '')} {row.get('owner_last_name', '')}".strip()
        if name:
            owners_by_account.setdefault(acc, []).append(name)

# 2. Fetch licenses
print("Fetching licenses...")
licenses_url = "https://data.cityofchicago.org/resource/r5kz-chrr.json"
r = requests.get(licenses_url, params={'$limit': 1500, 'city': 'CHICAGO', 'license_status': 'AAI'})
licenses_data = r.json()

properties = []
for row in licenses_data:
    acc = row.get('account_number')
    owner_names = owners_by_account.get(acc, [])
    owner_name = owner_names[0] if owner_names else row.get('legal_name')
    if not owner_name:
        continue
    
    properties.append({
        'bbl': row.get('license_id'),
        'address': row.get('address'),
        'borough': 'CHICAGO',
        'zip_code': row.get('zip_code'),
        'owner_name': owner_name,
        'owner_name_norm': owner_name.upper(),
        'mailing_address': row.get('address') or '',
        'assessed_total': None,
        'units_res': 1,
        'year_built': None,
        'land_use': row.get('license_description'),
        'bld_class': None,
        'latitude': float(row.get('latitude')) if row.get('latitude') else None,
        'longitude': float(row.get('longitude')) if row.get('longitude') else None
    })

print(f"Total matched properties: {len(properties)}")
for p in properties[:3]:
    print(p)
