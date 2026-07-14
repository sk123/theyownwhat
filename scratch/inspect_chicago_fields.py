import requests

url = "https://datacatalog.cookcountyil.gov/resource/bcnq-qi2z.json"
params = {
    '$limit': 3
}
r = requests.get(url, params=params)
data = r.json()
for row in data:
    print(row.keys())
