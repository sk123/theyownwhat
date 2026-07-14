import requests
import json

LAYER_URL = "https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/MapServer/1"

def test():
    print("Querying layer metadata...")
    try:
        response = requests.get(LAYER_URL, params={"f": "json"}, timeout=25)
        response.raise_for_status()
        data = response.json()
        print("Layer Name:", data.get("name"))
        print("Advanced Query Capabilities:")
        capabilities = data.get("advancedQueryCapabilities", {})
        print(json.dumps(capabilities, indent=2))
        
        # Check if supportsPagination is present
        print("supportsPagination:", capabilities.get("supportsPagination"))
        print("maxRecordCount:", data.get("maxRecordCount"))
        
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    test()
