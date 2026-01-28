import requests
from bs4 import BeautifulSoup
import re

def test_scrape(url):
    print(f"Testing scrape on {url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    data = {}
    
    # Try the labels we used in the script
    for label in ['Occupancy', 'Style']:
        label_td = soup.find('td', string=re.compile(f'^{label}$', re.IGNORECASE))
        if not label_td:
             # Try finding in all td's text
             label_td = soup.find(lambda tag: tag.name == 'td' and label.lower() in tag.text.lower().strip())
        
        if label_td:
            val_td = label_td.find_next_sibling('td')
            if val_td:
                val_text = val_td.text.strip()
                print(f"FOUND LABEL '{label}': '{val_text}'")
                if label == 'Occupancy':
                    try:
                        data['number_of_units'] = int(float(val_text))
                    except ValueError:
                        print(f"Failed to parse occupancy: {val_text}")
                elif label == 'Style':
                    data['property_type'] = val_text
            else:
                print(f"FOUND LABEL '{label}' but no next sibling td")
        else:
            print(f"LABEL '{label}' NOT FOUND")

    # Inspect all td pairs
    print("\nAll TD pairs in tables:")
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) >= 2:
            l = tds[0].text.strip()
            v = tds[1].text.strip()
            if l and v:
                print(f"  {l}: {v}")

    print("\nResult data:", data)

if __name__ == "__main__":
    # URL for 204 Cherry St, Milford
    test_scrape("https://gis.vgsi.com/milfordct/Parcel.aspx?pid=15873")
