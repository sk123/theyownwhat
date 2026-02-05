import requests
import json
import sys

def test_windsor_api():
    # User provided example: https://windsorct.com/sf/win/v1/propertycard/address/get?st_num=275&st_name=broad
    url = "https://windsorct.com/sf/win/v1/propertycard/address/get"
    params = {
        "st_num": "275",
        "st_name": "broad"
    }
    
    print(f"Querying: {url} with {params}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))
        else:
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    test_windsor_api()
