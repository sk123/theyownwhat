
import requests
import urllib3
from bs4 import BeautifulSoup
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Ansonia again (they have both?) - let's try Ashford
url = "http://propertyrecordcards.com/PropertySearch.aspx?towncode=003" # Ashford
try:
    resp = requests.get(url, verify=False, timeout=10)
    print(f"Status: {resp.status_code}")
    soup = BeautifulSoup(resp.text, 'html.parser')
    text = soup.get_text()
    if "update" in text.lower():
        print("Found 'update' in text!")
        for line in text.splitlines():
            if "update" in line.lower():
                print(f"-> {line.strip()}")
    else:
        print("No 'update' found in text.")
except Exception as e:
    print(f"Error: {e}")
