
import requests
from bs4 import BeautifulSoup

def debug_prop():
    url = "https://gis.vgsi.com/WinchesterCT/Parcel.aspx?pid=3678"
    print(f"Fetching: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers, verify=False)
    print(f"Status: {response.status_code}")
    
    with open('winchester_prop.html', 'w') as f:
        f.write(response.text)
    print("Saved winchester_prop.html")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Check for keywords
    for keyword in ['Owner', 'Location']:
        print(f"\nSearching for '{keyword}'...")
        # Search all tags
        elements = soup.find_all(string=lambda text: text and keyword in text)
        print(f"Found {len(elements)} string matches.")
        for el in elements[:3]:
            parent = el.parent
            print(f" - Match in <{parent.name}>: '{el.strip()}'")
            print(f"   Parent HTML: {parent}")

if __name__ == "__main__":
    debug_prop()
