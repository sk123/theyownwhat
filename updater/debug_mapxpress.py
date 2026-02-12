import requests
from bs4 import BeautifulSoup

def debug_farmington():
    # Farmington is MapXpress
    base_url = "https://farmington.mapxpress.net/PAGES/detail.asp"
    # Need a valid ID. Usually these are sequential integers or MBLs.
    # Let's try to find one via search first to be sure.
    
    search_url = "https://farmington.mapxpress.net/PAGES/search.asp"
    session = requests.Session()
    
    # 1. Search for a property (e.g. "Main St")
    print("Searching for properties...")
    resp = session.post(search_url, data={
        'searchname': 'MAIN ST',
        'houseno': '',
        'mbl': '',
        'go.x': 1, 'go.y': 1
    })
    
    if resp.status_code != 200:
        print(f"Search failed: {resp.status_code}")
        return

    soup = BeautifulSoup(resp.content, 'html.parser')
    links = soup.find_all('a', href=lambda h: h and 'detail.asp' in h)
    
    if not links:
        print("No search results found.")
        print(resp.text[:500])
        return

    first_link = links[0]['href']
    full_url = f"https://farmington.mapxpress.net/PAGES/{first_link}"
    print(f"Found property URL: {full_url}")
    
    # 2. Fetch Detail Page
    resp = session.get(full_url)
    print(f"Detail Page Status: {resp.status_code}")
    
    # Dump HTML to file
    with open("farmington_debug.html", "w") as f:
        f.write(resp.text)
    print("Saved HTML to farmington_debug.html")

    # 4. Import and test the new parser
    try:
        from updater.update_data import parse_mapxpress_html
        print("\\n--- Testing parse_mapxpress_html ---")
        data = parse_mapxpress_html(resp.text)
        print(f"Extracted Data: {data}")
        
        if 'image_url' in data:
            print(f"SUCCESS: Found Image URL: {data['image_url']}")
        else:
            print("FAILURE: No Image URL found")
            
        if 'year_built' in data:
             print(f"SUCCESS: Found Year Built: {data['year_built']}")
        else:
             print("FAILURE: No Year Built found")

    except ImportError:
        print("Could not import updater.update_data (running standalone?)")
        # Copy-paste parser logic for standalone test if needed, but we are in repo
        pass

if __name__ == "__main__":
    debug_farmington()
