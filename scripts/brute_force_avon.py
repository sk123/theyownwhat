
import requests
import json

SERVICES = [
    "Avon_Public_Viewer_Layers",
    "Avon_Management_Layers",
    "Avon_Parcels",
    "Avon_GIS",
    "Avon_Web_Layers",
    "Avon_Assessor",
    "Public_Viewer_Layers", # Sometimes simpler
]

BASE_URL = "https://hosting.tighebond.com/arcgis/rest/services/AvonCT"

def check_query(service_name):
    # Try querying layer 0, 1, 2...
    for layer_id in range(10):
        url = f"{BASE_URL}/{service_name}/MapServer/{layer_id}/query"
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultRecordCount": 1,
            "f": "json"
        }
        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if 'features' in data and len(data['features']) > 0:
                    print(f"!!! FOUND IT !!!")
                    print(f"URL: {url}")
                    print(f"Sample Feature: {data['features'][0]}")
                    return True
                elif 'error' in data:
                    # e.g. layer does not exist
                    # print(f"Error on {service_name}/{layer_id}: {data['error']['message']}")
                    pass
        except Exception as e:
            pass
    return False

def main():
    print("Brute-force querying Avon services...")
    for s in SERVICES:
        print(f"Checking {s}...")
        if check_query(s):
            break

if __name__ == "__main__":
    main()
