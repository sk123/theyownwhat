
from bs4 import BeautifulSoup
import re

def test():
    with open('winchester_prop.html', 'r') as f:
        content = f.read()
    
    soup = BeautifulSoup(content, 'html.parser')
    
    selectors = {
        'owner': ['span[id*="lblGenOwner"]', 'span[id*="lblOwner"]'],
        'location': ['*[id="MainContent_lblLocation"]', '*[id*="lblLocation"]', '*[id*="lblGenLocation"]']
    }
    
    data = {}
    for field, field_selectors in selectors.items():
        for selector in field_selectors:
            element = soup.select_one(selector)
            if element:
                print(f"Found {field} with selector '{selector}': '{element.text.strip()}'")
                data[field] = element.text.strip()
                break
        if field not in data:
            print(f"Failed to find {field}")

if __name__ == "__main__":
    test()
