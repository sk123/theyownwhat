
import requests
from bs4 import BeautifulSoup

url = "https://gis.vgsi.com/newhavenct/Parcel.aspx?pid=5466"

import warnings
warnings.filterwarnings("ignore")

try:
    print(f"Fetching {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers, timeout=20, verify=False)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for the Location label
        # Trying typical IDs
        ids_to_check = ['MainContent_lblLocation', 'lblLocation', 'lblGenLocation', 'MainContent_lblGenLocation', 'MainContent_lblLocationAddress']
        
        found = False
        for i in ids_to_check:
            el = soup.find(id=i) # exact ID match first
            if not el:
                # partial match
                el = soup.select_one(f'*[id*="{i}"]')
            
            if el:
                print(f"Found ID '{i}': text='{el.get_text(strip=True)}'")
                print(f"HTML: {el}")
                found = True
        
        if not found:
             # Dump partial HTML
             print("Location ID not found. Dumping <body> start...")
             print(str(soup.body)[:500])

except Exception as e:
    print(f"Error: {e}")
