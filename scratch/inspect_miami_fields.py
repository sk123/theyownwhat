import requests

url = "https://gisweb.miamidade.gov/arcgis/rest/services/MD_LandInformation/MapServer/26/query"
params = {
    'where': '1=1',
    'outFields': '*',
    'resultRecordCount': 1,
    'f': 'json'
}
r = requests.get(url, params=params)
data = r.json()
features = data.get('features', [])
if features:
    attrs = features[0].get('attributes', {})
    for k, v in attrs.items():
        print(f"{k}: {v} ({type(v)})")
else:
    print("No features found")
