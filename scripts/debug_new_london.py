
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def debug_new_london():
    url = "https://gis.vgsi.com/newlondonct/Streets.aspx?Letter=A"
    print(f"Fetching New London Letter A: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        with open('new_london_debug.html', 'w') as f:
            f.write(response.text)
        print("Saved new_london_debug.html")

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find street links
        street_links = soup.find_all('a', href=lambda h: h and 'Streets.aspx?Name=' in h)
        print(f"Found {len(street_links)} street links.")
        for link in street_links[:5]:
            print(f" - {link.get('href')} : {link.text.strip()}")
            
        if street_links:
            street_url = urljoin(url, street_links[0].get('href'))
            print(f"\nFetching first street: {street_url}")
            resp = requests.get(street_url, headers=headers, verify=False)
            
            s_soup = BeautifulSoup(resp.content, 'html.parser')
            prop_links = s_soup.select("a[href*='.aspx?pid='], a[href*='.aspx?acct=']")
            print(f"Found {len(prop_links)} property links on street.")
            for p_link in prop_links[:5]:
                print(f" - {p_link.get('href')} : {p_link.text.strip()}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_new_london()
