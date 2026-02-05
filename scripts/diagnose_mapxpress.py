
import requests
from bs4 import BeautifulSoup
import re

def test_mapxpress_fetch(domain, unique_id):
    base_url = f"https://{domain}"
    search_url = f"{base_url}/PAGES/search.asp"
    detail_url = f"{base_url}/PAGES/detail.asp?UNIQUE_ID={unique_id}"
    
    session = requests.Session()
    session.verify = False
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': search_url,
        'Origin': base_url
    })
    
    print(f"Initializing session at {search_url}...")
    session.get(search_url, timeout=10)
    
    print(f"Searching for 'A'...")
    resp = session.post(search_url, data={
        'searchname': 'A',
        'houseno': '',
        'mbl': '',
        'go.x': 1, 'go.y': 1
    }, timeout=20)
    
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = soup.find_all('a', href=lambda h: h and 'detail.asp' in h)
        print(f"Found {len(links)} search results.")
        if links:
            first_link = links[0].get('href')
            print(f"First link: {first_link}")
            detail_url = f"{base_url}/PAGES/{first_link}"
    
    print(f"Fetching detail page {detail_url}...")
    resp = session.get(detail_url, timeout=15)
    
    if resp.status_code == 200:
        print("Success! HTML Length:", len(resp.text))
        with open('scripts/test_detail.html', 'w') as f:
            f.write(resp.text)
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Diagnostics
        print("\n--- Diagnostic Results ---")
        
        # 1. Look for images
        imgs = soup.find_all('img')
        print(f"Total <img> tags found: {len(imgs)}")
        for i, img in enumerate(imgs):
            print(f"  Img {i}: id={img.get('id')}, src={img.get('src')}")
            
        # 2. Check existing scraper selectors
        photo_img = soup.find('img', id=re.compile(r'Photo|MainImage', re.I))
        if not photo_img:
            photo_img = soup.find('img', src=re.compile(r'/photos/|/images/prop', re.I))
        
        if photo_img:
            print(f"SCAPER FOUND PHOTO: {photo_img.get('src')}")
        else:
            print("SCRAPER FAILED TO FIND PHOTO with current logic.")
            
        # 3. Check for other fields like Living Area, Year Built
        print("\nChecking for fields in tables...")
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    for i in range(len(cells)-1):
                        text = cells[i].get_text(strip=True)
                        val = cells[i+1].get_text(strip=True)
                        if any(k in text for k in ["Living Area", "Year Built", "Style", "Occupancy"]):
                            print(f"  Found Field: '{text}' = '{val}'")

    else:
        print(f"Failed with status code: {resp.status_code}")
        print(resp.text[:500])

if __name__ == "__main__":
    test_mapxpress_fetch('guilford.mapxpress.net', '10161')
