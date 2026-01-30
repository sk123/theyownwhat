
import requests
from bs4 import BeautifulSoup
import re
import sys

# Scrape logic copied from update_data.py
def scrape_individual_property_page(prop_page_url, session):
    try:
        response = session.get(prop_page_url, verify=False, timeout=20)
        soup = BeautifulSoup(response.content, 'html.parser')
        data = {}
        
        # 1. Table-based extraction (MainContent_ctl01_fvData)
        main_table = soup.find('table', {'id': 'MainContent_ctl01_fvData'})
        if main_table:
            # Map labels to our keys
            mapping = {
                'Location': 'location',
                'Owner': 'owner',
                'Assessment': 'assessed_value',
                'Appraisal': 'appraised_value',
                'Sale Price': 'sale_amount',
                'Sale Date': 'sale_date',
                'Year Built': 'year_built',
                'Living Area': 'living_area',
                'Acres': 'acres',
                'Zone': 'zone',
                'Use Code/Description': 'property_type'
            }
            
            for row in main_table.find_all('tr'):
                cells = row.find_all('td')
                for cell in cells:
                    # Look for labels inside bold or just text
                    # The layout is usually Label: Value or Label <br> Value
                    # This generic parser is what update_data uses
                     
                    # We can try to just get all pairs
                    # West Hartford specific check:
                    pass
        
        # 2. Generic key-value pair finder (fallback)
        # Find all cells with class "lbl" or similar? 
        # Actually update_vision_data uses a loop over mapping.
        
        fallback_keywords = {
            'owner': 'Owner',
            'sale_amount': 'Sale Price',
            'sale_date': 'Sale Date',
            'assessed_value': 'Assessment',
            'appraised_value': 'Appraisal',
            'location': 'Location',
            'unit': 'Unit'
        }

        print("--- Extracted Pairs ---")
        for field, keyword in fallback_keywords.items():
            # Try finding the label
            # Regular expression for case-insensitive match
            element = soup.find(string=re.compile(f'^{keyword}', re.IGNORECASE))
            if element:
                # Usually the value is in the next sibling or parent's next sibling
                # Case 1: <td>Label</td><td>Value</td>
                # Case 2: <span class="lbl">Label</span> <span class="val">Value</span>
                
                # Check parent
                parent = element.parent
                val = None
                
                # Try next sibling of parent (<td> -> <td>)
                if parent.name in ['td', 'span', 'div']:
                    next_sib = parent.find_next_sibling(['td', 'span', 'div'])
                    if next_sib:
                        val = next_sib.get_text(strip=True)
                
                if val:
                    print(f"Found {field}: {val}")
                    data[field] = val
        
        return data
    except Exception as e:
        print(f"Error: {e}")
        return None

def debug_scrape_prop():
    url = "https://gis.vgsi.com/westhartfordct/Parcel.aspx?pid=47"
    print(f"Scraping {url}...")
    with requests.Session() as s:
        s.headers.update({'User-Agent': 'Mozilla/5.0'})
        data = scrape_individual_property_page(url, s)
        print("\nFinal Data:")
        print(data)

if __name__ == "__main__":
    debug_scrape_prop()
