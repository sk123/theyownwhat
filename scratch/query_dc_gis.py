import requests

url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer/40/query"

for where in ["PREMISEADD LIKE '%524 46TH%'", "PREMISEADD LIKE '%2312 GREEN%'"]:
    params = {
        'where': where,
        'outFields': 'SSL,OWNERNAME,PREMISEADD,ADDRESS1,CITYSTZIP,ASSESSMENT,PRMSWARD,USECODE',
        'returnGeometry': 'false',
        'f': 'json'
    }
    r = requests.get(url, params=params)
    data = r.json()
    print("Where:", where)
    print("Features count:", len(data.get('features', [])))
    for f in data.get('features', []):
        print(f.get('attributes'))
