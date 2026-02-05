
import requests
import json

BASE_PATTERNS = [
    "https://hosting.tighebond.com/arcgis/rest/services/AvonCT",
    "https://hosting.tighebond.com/arcgis/rest/services/AvonCT/Public_Viewer_Layers",
    "https://hosting.tighebond.com/arcgis/rest/services/AvonCT/Avon_Dynamic_Layers",
    "https://hosting.tighebond.com/arcgis/rest/services/AvonCT/Community_Explorer",
]

def check_url(url):
    try:
        # Check folder or service JSON
        resp = requests.get(f"{url}?f=json", timeout=5)
        if resp.status_code == 200:
            print(f"[FOUND] {url}")
            try:
                data = resp.json()
                # print(data)
                if 'services' in data:
                    print("  -> Services:")
                    for s in data['services']:
                        print(f"     - {s['name']} ({s['type']})")
                if 'layers' in data:
                    print("  -> Layers:")
                    for l in data['layers']:
                        print(f"     - {l['id']}: {l['name']}")
            except:
                pass
            return True
        else:
            print(f"[404] {url}")
    except Exception as e:
        print(f"[ERR] {url}: {e}")
    return False

def main():
    print("Probing Tighe & Bond ArcGIS Server (Round 3)...")
    
    candidates = [
        "https://hosting.tighebond.com/arcgis/rest/services/AvonCT_public",
        "https://hosting.tighebond.com/arcgis/rest/services/AvonCT_Public",
        "https://hosting.tighebond.com/arcgis/rest/services/AvonCT_public/MapServer",
        "https://hosting.tighebond.com/arcgis/rest/services/AvonCT_public/Avon_Public_Viewer_Layers/MapServer",
        "https://hosting.tighebond.com/arcgis/rest/services/AvonCT/Avon_Public_Viewer_Layers/MapServer", # Check again
    ]

    for url in candidates:
        check_url(url)



if __name__ == "__main__":
    main()
