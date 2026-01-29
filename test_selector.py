
from bs4 import BeautifulSoup

def test():
    with open('winchester_debug.html', 'r') as f:
        content = f.read()
    
    soup = BeautifulSoup(content, 'html.parser')
    
    selector = "a[href*='.aspx?pid='], a[href*='.aspx?acct=']"
    prop_links = soup.select(selector)
    
    print(f"Selector: {selector}")
    print(f"Found {len(prop_links)} links.")
    for link in prop_links[:5]:
        print(f" - {link.get('href')} : {link.text}")

if __name__ == "__main__":
    test()
