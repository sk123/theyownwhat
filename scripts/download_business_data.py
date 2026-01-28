
import requests
import os

SOURCES = {
    "businesses.csv": "https://data.ct.gov/api/views/65f3-5xat/rows.csv?accessType=DOWNLOAD",
    "principals.csv": "https://data.ct.gov/api/views/9z8b-f4as/rows.csv?accessType=DOWNLOAD"
}

# Note: The user provided catalog.data.gov links, but the direct CSV export links are usually more stable for scripts.
# Catalog links:
# https://catalog.data.gov/dataset/connecticut-business-registry-business-master
# https://catalog.data.gov/dataset/connecticut-business-registry-principals

DATA_DIR = "data"

def download():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    for filename, url in SOURCES.items():
        print(f"Downloading {filename} from {url}...")
        try:
            r = requests.get(url, stream=True, timeout=60, verify=False)
            r.raise_for_status()
            with open(os.path.join(DATA_DIR, filename), 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Successfully downloaded {filename}")
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            return False
    return True

if __name__ == "__main__":
    download()
