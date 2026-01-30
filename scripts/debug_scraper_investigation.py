
import requests
from bs4 import BeautifulSoup
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_new_haven_prop():
    pid = "12419"
    url = f"https://gis.vgsi.com/newhavenct/Parcel.aspx?pid={pid}"
    print(f"Fetching New Haven Property: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Test dt/dd parsing logic
        data = {}
        for dl in soup.find_all('dl'):
            dt = dl.find('dt')
            dd = dl.find('dd')
            if dt and dd:
                key = dt.get_text(strip=True).replace(':', '')
                val = dd.get_text(strip=True)
                data[key] = val
                print(f"Found dt/dd: {key} -> {val}")
        
        location = data.get('Location', '')
        print(f"Extracted Location: {location}")
        
        # Try fallbacks from update_data.py
        # KEYWORDS including 'Unit', 'Location'
        
        # Regex for unit in location
        # "570 WHITNEY AV B2" -> Unit B2?
        match = re.search(r'(?:UNIT|#|APT|STE)\s*([A-Z0-9-]+)', location, re.IGNORECASE)
        if match:
             print(f"Regex Unit from Location: {match.group(1)}")
        else:
             # Try implicit suffix
             print(f"No explicit unit marker in location '{location}'")
             # Try to parse it manually like PropertyTable
             parts = location.split()
             if len(parts) > 1:
                 print(f"Potential implicit unit: {parts[-1]}")

    except Exception as e:
        print(f"Error fetching New Haven: {e}")

def test_west_hartford_street():
    # West Hartford street listing
    # We need a street that exists. "MAIN ST" is usually safe.
    # Note: West Hartford site is west_hartford_ct_vision (?) or similar.
    # Actually URL base is https://gis.vgsi.com/westhartfordct/
    # Fetch New London Letter A page
    url = "https://gis.vgsi.com/newlondonct/Streets.aspx?Letter=A"
    print(f"\nFetching New London Letter A: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        with open('winchester_letter.html', 'w') as f:
            f.write(response.text)
        print("Saved winchester_letter.html")

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Check for street links
        # Looking for <a href="StreetListing.aspx?StrtName=...">
        links = soup.find_all('a', href=re.compile(r'StreetListing\.aspx'))
        print(f"Found {len(links)} street links.")
        for link in links[:5]:
            print(f" - {link.get_text()} ({link['href']})")
            
        if not links:
            # Maybe it redirects to Main St listing if only one match?
            # Or maybe "Streets.aspx" lists streets matching "MAIN"
            pass

    except Exception as e:
        print(f"Error fetching West Hartford Street: {e}")

if __name__ == "__main__":
    test_new_haven_prop()
    test_west_hartford_street()
