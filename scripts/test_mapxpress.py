
import requests
import urllib3
from bs4 import BeautifulSoup
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://ansonia.mapxpress.net/"
try:
    resp = requests.get(url, verify=False, timeout=10)
    print(f"Status: {resp.status_code}")
    soup = BeautifulSoup(resp.text, 'html.parser')
    text = soup.get_text()
    if "update" in text.lower():
        print("Found 'update' in text!")
        # Print lines containing 'update'
        for line in text.splitlines():
            if "update" in line.lower():
                print(f"-> {line.strip()}")
    else:
        print("No 'update' found in text.")
except Exception as e:
    print(f"Error: {e}")
