
import requests
import json
import re
import sys

# Cromwell MapGeo
BASE_URL = "https://cromwellct.mapgeo.io"
SEARCH_API = f"{BASE_URL}/api/ui/datasets/properties"

def get_layout_id(session):
    print("Fetching main page to find layoutId...")
    resp = session.get(BASE_URL)
    if resp.status_code != 200:
        print(f"Failed to fetch main page: {resp.status_code}")
        return None
    
    # MapGeo usually embeds config in a script tag or similar
    # Look for "layoutId" or a UUID pattern near "properties"
    # Or sometimes it's in a main.js file. 
    # Let's try a simple regex for the config JSON often found in index.html
    # It might be in window.MAPGEO_CONFIG or similar.
    
    # Strategy 1: Look for mapgeo config in HTML
    match = re.search(r'layoutId["\']?:\s*["\']([a-f0-9\-]+)["\']', resp.text)
    if match:
        return match.group(1)
        
    # Strategy 2: Look for any UUID associated with datasets
    # This is a bit looser.
    print("Could not find explicit layoutId in HTML. Dumping some context:")
    print(resp.text[:500])
    return None

def test_mapgeo():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': BASE_URL + '/',
        'Origin': BASE_URL
    })

    layout_id = get_layout_id(session)
    # Fallback to the one seen in browser logs if extraction fails (validating if it's static)
    if not layout_id:
        print("Using fallback layoutID from research...")
        layout_id = "6032ef5b-8331-4abb-aa9d-e1a114b21443" 

    print(f"Using Layout ID: {layout_id}")

    # MapGeo uses a POST request for search
    print("\nTesting Search for 'Main' using POST...")
    payload = {
        "attributes": ["displayName", "ownerName", "id"],
        "query": {
            "displayName": "Main",
            "id": "",
            "ownerName": ""
        },
        "geometry": {
            "type": "FeatureCollection",
            "features": []
        },
        "sort": []
    }
    
    # We might not need layoutId for this endpoint, but let's keep headers clean
    resp = session.post(
        "https://cromwellct.mapgeo.io/api/datasets/properties/search?format=json",
        json=payload
    )
    
    if resp.status_code == 200:
        data = resp.json()
        # API returns a list directly
        items = data if isinstance(data, list) else data.get('items', [])
        
        print(f"Search found {len(items)} results.")
        if items:
            first = items[0]
            print("First result keys:", first.keys())
            print("Sample ID:", first.get('id'))
            print("Row ID (likely primary):", first.get('rowId'))
            print("Owner:", first.get('ownerName'))
            
            # Now test detail fetch using the ID
            # The previous GET detail might work with this ID
            prop_id = first.get('id')
            if prop_id:
                print(f"\nTesting Detail Fetch for {prop_id}...")
                # Try the ItemDetails layout approach again, or find another way
                # Research showed: GET .../properties/{id}?layoutId=...&layoutType=itemDetails
                detail_url = f"https://cromwellct.mapgeo.io/api/ui/datasets/properties/{prop_id}"
                detail_params = {
                    'layoutId': layout_id, # We might still need this for details
                    'layoutType': 'itemDetails'
                }
                detail_resp = session.get(detail_url, params=detail_params)
                if detail_resp.status_code == 200:
                    detail_data = detail_resp.json()
                    # print("Detail Data Keys:", detail_data.keys())
                    print("Detail Data Owner:", detail_data.get('data', {}).get('ownerName'))
                else:
                    print(f"Detail fetch failed: {detail_resp.status_code}")
    else:
        print(f"Search failed: {resp.status_code}")
        print(resp.text)

if __name__ == "__main__":
    test_mapgeo()
