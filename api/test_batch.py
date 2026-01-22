
import requests
import sys
import time

def test():
    print("Testing Batch Geocode with hardcoded IDs...", flush=True)
    try:
        # Use simple IDs known to likely exist or just test endpoints handling of 1
        ids = ["1", "2", "3"]
            
        print(f"Testing batch geocode for IDs: {ids}", flush=True)
        r2 = requests.post("http://localhost:8000/api/geocoding/batch", json={"property_ids": ids})
        print(f"Status Code: {r2.status_code}")
        r2.raise_for_status()
        res = r2.json()
        print(f"Batch geocode returned {len(res)} results.", flush=True)
        if res:
            print(f"Sample: {res[0]}", flush=True)
        else:
            print("Batch returned empty list (IDs might not need geocoding or not exist)", flush=True)
        
    except Exception as e:
        print(f"Test failed: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    test()
