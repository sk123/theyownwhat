
import requests
from bs4 import BeautifulSoup
import re

def parse_propertyrecordcards_html(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    data = {}
    
    # Helper to clean currency/number strings
    def clean_number(val):
        if not val: return None
        return re.sub(r'[^\d.]', '', val)

    # 1. Parse IDs (Acres, Zone, Values)
    id_map = {
        'acres': 'MainContent_tbgMapAcres',
        'zone': 'MainContent_tbgMapZone',
        'appraised_value': 'MainContent_tbgMapAppraisedValue',
        'assessed_value': 'MainContent_tbgMapAssessedValue'
    }

    for db_field, html_id in id_map.items():
        element = soup.find(id=html_id)
        if element and element.get('value'):
            val = element.get('value')
            print(f"DEBUG: Found {db_field} -> {val}")
            try:
                if db_field in ['acres', 'appraised_value', 'assessed_value']:
                    clean_val = clean_number(val)
                    if clean_val:
                        data[db_field] = float(clean_val)
                else:
                    data[db_field] = val
            except: pass

    # 2. Parse Tables (Living Area, Year Built, Style, Units)
    key_map = {
        'Living Area:': 'living_area',
        'Year Built:': 'year_built',
        'Style:': 'property_type',
        'Use Code:': 'property_type', 
        'Unit:': 'unit',
        'Total Rooms:': 'total_rooms',
        'Total Bedrms:': 'total_bedrooms',
        'Total Baths:': 'total_baths'
    }
    
    tables = soup.find_all('table')
    for table in tables:
        cells = table.find_all('td')
        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            if text in key_map and i + 1 < len(cells):
                val = cells[i+1].get_text(strip=True)
                db_field = key_map[text]
                print(f"DEBUG: Tbl Found {db_field} -> {val}")
                
                try:
                    if db_field == 'living_area':
                         clean_val = clean_number(val)
                         if clean_val: data[db_field] = float(clean_val)
                    elif db_field == 'year_built':
                        data[db_field] = int(val)
                    elif db_field in ['total_rooms', 'total_bedrooms', 'total_baths']:
                        data[db_field] = int(val)
                    else:
                        data[db_field] = val
                except: pass
                
    # 3. Find Photo
    # Look for img with class "img-thumbnail" or similar, or inside a specific container
    img_tag = soup.find('img', id='MainContent_imgProperty')
    if img_tag:
        src = img_tag.get('src')
        print(f"DEBUG: Found Photo -> {src}")
        if src and 'nophoto' not in src.lower():
            if src.startswith('http'):
                data['building_photo'] = src
            else:
                 # It's likely relative
                 data['building_photo'] = f"https://www.propertyrecordcards.com{src}"

    return data

def test_waterbury(unique_id):
    url = f"https://www.propertyrecordcards.com/propertyresults.aspx?towncode=151&uniqueid={unique_id}"
    print(f"Fetching {url}...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    resp = requests.get(url, headers=headers)
    
    if resp.status_code == 200:
        with open("waterbury_details.html", "w") as f:
            f.write(resp.text)
        
        soup = BeautifulSoup(resp.text, 'lxml')
        imgs = soup.find_all('img')
        print(f"DEBUG: Found {len(imgs)} images.")
        for img in imgs:
            print(f"IMG: src={img.get('src')} id={img.get('id')} class={img.get('class')}")

        data = parse_propertyrecordcards_html(resp.text)
        print("Parsed Data:", data)
    else:
        print(f"Failed: {resp.status_code}")

if __name__ == "__main__":
    # Test local file
    with open("waterbury_details.html", "r") as f:
        html = f.read()
    
    soup = BeautifulSoup(html, 'lxml')
    imgs = soup.find_all('img')
    print(f"DEBUG: Found {len(imgs)} images.")
    anchors = soup.find_all('a', class_='thumbnail')
    print(f"DEBUG: Found {len(anchors)} thumbnail anchors.")
    for a in anchors:
        print(f"ANCHOR: href={a.get('href')} class={a.get('class')}")
