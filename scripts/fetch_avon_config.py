
import requests

BASE = "https://hosting.tighebond.com/AvonCT_public"
PATHS = [
    "config.json",
    "config/config.json",
    "js/config.js",
    "proxy/proxy.config",
    "app/config.js",
    "index.html" # Maybe in source?
]

def check_config():
    for p in PATHS:
        url = f"{BASE}/{p}"
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                print(f"[FOUND] {url}")
                # print(r.text[:500])
                print(r.text) # PRINT ALL
                if "MapServer" in r.text:
                    print("!!! FOUND MapServer URL IN CONFIG !!!")
                    start = r.text.find("http", r.text.find("MapServer") - 100)
                    end = r.text.find("MapServer", start) + 9
                    print(r.text[start:end+20])
            else:
                print(f"[404] {url}")
        except Exception as e:
            print(f"[ERR] {url}: {e}")

if __name__ == "__main__":
    check_config()
