
import requests
import re
from bs4 import BeautifulSoup

def verify_strip():
    headers = {'User-Agent': 'Mozilla/5.0'}
    url = "https://gis.vgsi.com/meridenct/Streets.aspx?Name=ABBEY PARK" # Explicitly NO trailing space
    
    print(f"Fetching: '{url}'")
    resp = requests.get(url, headers=headers, verify=False)
    print(f"Len: {len(resp.text)}")
    soup = BeautifulSoup(resp.content, 'html.parser')
    
    # Scraper regex
    prop_links = soup.find_all("a", href=re.compile(r'\.aspx\?(pid|acct|uniqueid)=', re.I))
    print(f"Links found: {len(prop_links)}")
    for l in prop_links[:3]:
        print(f"Sample: {l.text.strip()} -> {l['href']}")

if __name__ == "__main__":
    verify_strip()
